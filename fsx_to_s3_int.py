import boto3
import argparse
from datetime import datetime, timedelta, timezone

# Constants
BYTES_TO_TB = 1_099_511_627_776  # 1 TB = 1024^4 bytes
BYTES_TO_GB = 1_073_741_824      # 1 GB = 1024^3 bytes
BYTES_TO_MB = 1_048_576          # 1 MB = 1024^2 bytes
SECONDS_IN_DAY = 86400
HOUR = 3600  # 1 hour in seconds
DAY = 86400  # 24 hours in seconds
HOURS_PER_MONTH = 730   # Average number of hours in a month
DAYS_PER_MONTH = 30    # Average days in a month

def get_metric(client, metric_name, fsx_id, volume_id, days=1, stat="Average", storage_tier=None, data_type=None, period=None):
    """Fetches CloudWatch metrics for the FSx volume."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)
    
    # Calculate appropriate period based on time range to stay under 1440 datapoints
    if period is None:
        min_period = (days * 24 * 60 * 60) // 1400
        period = max(300, ((min_period + 59) // 60) * 60)
    
    dimensions = [
        {"Name": "FileSystemId", "Value": fsx_id},
        {"Name": "VolumeId", "Value": volume_id}
    ]
    
    # Only add StorageTier for specific metrics
    if storage_tier and metric_name in ["StorageUsed", "DataReadBytes", "DataWriteBytes"]:
        dimensions.append({"Name": "StorageTier", "Value": storage_tier})
    
    # Only add DataType for StorageUsed
    if data_type and metric_name == "StorageUsed":
        dimensions.append({"Name": "DataType", "Value": data_type})
    
    response = client.get_metric_statistics(
        Namespace="AWS/FSx",
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start_time.isoformat(),
        EndTime=end_time.isoformat(),
        Period=period,
        Statistics=[stat]
    )
    
    if not response.get("Datapoints"):
        return 0
        
    if stat == "Sum":
        total = sum(point[stat] for point in response["Datapoints"])
        if "TotalClientThroughput" in metric_name:
            # Convert bytes to GB and account for the period
            return (total * period) / BYTES_TO_GB
        elif "Bytes" in metric_name or metric_name in ["StorageCapacity", "StorageUsed"]:
            return total / BYTES_TO_GB  # Convert bytes to GB
        return total
    else:
        latest_point = max(response["Datapoints"], key=lambda x: x['Timestamp'])
        if "TotalClientThroughput" in metric_name:
            # Convert bytes/second to GB for the period
            return (latest_point[stat] * period) / BYTES_TO_GB
        elif "Bytes" in metric_name or metric_name in ["StorageCapacity", "StorageUsed"]:
            return latest_point[stat] / BYTES_TO_GB  # Convert bytes to GB
        return latest_point[stat]

def get_storage_metrics(client, fsx_id, volume_id):
    """Gets detailed storage metrics matching the CloudWatch console."""
    
    # Get total storage capacity
    storage_capacity = get_metric(client, "StorageCapacity", fsx_id, volume_id, 
                                stat="Average", storage_tier=None, data_type=None)
    
    # Get user data storage
    user_storage = get_metric(client, "StorageUsed", fsx_id, volume_id,
                            stat="Average", storage_tier="All", data_type="User")
    
    # Get snapshot data storage
    snapshot_storage = get_metric(client, "StorageUsed", fsx_id, volume_id,
                                stat="Average", storage_tier="All", data_type="Snapshot")
    
    # Get other data storage
    other_storage = get_metric(client, "StorageUsed", fsx_id, volume_id,
                             stat="Average", storage_tier="All", data_type="Other")
    
    return {
        'capacity': storage_capacity,
        'user_data': user_storage,
        'snapshot_data': snapshot_storage,
        'other_data': other_storage,
        'available': storage_capacity - user_storage - snapshot_storage - other_storage,
        'utilization': (user_storage + snapshot_storage + other_storage) / storage_capacity * 100 if storage_capacity > 0 else 0,
        'files_capacity': get_metric(client, "FilesCapacity", fsx_id, volume_id, stat="Maximum")
    }

def get_throughput_metric(client, fsx_id, volume_id, days=1):
    """Fetches combined read and write throughput for the period."""
    # Limit days to 14 since that's our max historical data
    days = min(days, 14)
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)
    
    # Calculate appropriate period
    min_period = (days * 24 * 60 * 60) // 1400
    period = max(300, ((min_period + 59) // 60) * 60)
    
    dimensions = [
        {"Name": "FileSystemId", "Value": fsx_id},
        {"Name": "VolumeId", "Value": volume_id}
    ]
    
    # Get read bytes
    read_response = client.get_metric_statistics(
        Namespace="AWS/FSx",
        MetricName="DataReadBytes",
        Dimensions=dimensions,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
        Statistics=["Sum"]
    )
    
    # Get write bytes
    write_response = client.get_metric_statistics(
        Namespace="AWS/FSx",
        MetricName="DataWriteBytes",
        Dimensions=dimensions,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
        Statistics=["Sum"]
    )
    
    # Sum up total bytes transferred
    total_bytes = 0
    if read_response.get("Datapoints"):
        total_bytes += sum(point["Sum"] for point in read_response["Datapoints"])
    if write_response.get("Datapoints"):
        total_bytes += sum(point["Sum"] for point in write_response["Datapoints"])
    
    return total_bytes / BYTES_TO_GB

def get_select_metrics(client, fsx_id, volume_id, days=14):
    """Calculates S3 Select-like metrics from FSx read/write operations."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)
    
    # Calculate period to stay under 1440 datapoints
    # Total seconds / 1440 = minimum period
    total_seconds = days * 24 * 60 * 60
    min_period = total_seconds // 1440
    # Round up to nearest 60 seconds
    period = max(300, ((min_period + 59) // 60) * 60)
    
    dimensions = [
        {"Name": "FileSystemId", "Value": fsx_id},
        {"Name": "VolumeId", "Value": volume_id}
    ]
    
    # Get data read bytes
    read_bytes_response = client.get_metric_statistics(
        Namespace="AWS/FSx",
        MetricName="DataReadBytes",
        Dimensions=dimensions,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
        Statistics=["Sum"]
    )
    
    # Calculate totals
    total_read = sum(point["Sum"] for point in read_bytes_response.get("Datapoints", [])) / BYTES_TO_GB
    
    # Calculate averages and projections
    daily_scanned = total_read / days
    monthly_scanned = daily_scanned * DAYS_PER_MONTH
    monthly_returned = monthly_scanned * 0.3  # 30% estimate
    
    return monthly_scanned, monthly_returned

def main():
    parser = argparse.ArgumentParser(description="Calculate FSx to S3 INT metrics")
    parser.add_argument("--fsx-id", required=True, help="FSx File System ID")
    parser.add_argument("--volume-id", required=True, help="FSx Volume ID")
    parser.add_argument("--region", required=True, help="AWS Region")
    parser.add_argument("--profile", required=True, help="AWS Profile")
    args = parser.parse_args()

    # Initialize AWS session
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cloudwatch = session.client("cloudwatch")

    # Get metrics
    storage_metrics = get_storage_metrics(cloudwatch, args.fsx_id, args.volume_id)
    files_used = get_metric(cloudwatch, "FilesUsed", args.fsx_id, args.volume_id, stat="Average")
    write_ops = get_metric(cloudwatch, "DataWriteOperations", args.fsx_id, args.volume_id, stat="Sum", period=300)
    read_ops = get_metric(cloudwatch, "DataReadOperations", args.fsx_id, args.volume_id, stat="Sum", period=300)
    metadata_ops = get_metric(cloudwatch, "MetadataOperations", args.fsx_id, args.volume_id, stat="Sum", period=300)
    data_read_bytes_7d = get_throughput_metric(cloudwatch, args.fsx_id, args.volume_id, days=7)
    data_read_bytes_14d = get_throughput_metric(cloudwatch, args.fsx_id, args.volume_id, days=14)
    monthly_data_scanned, monthly_data_returned = get_select_metrics(cloudwatch, args.fsx_id, args.volume_id)

    # Calculate daily averages
    daily_avg_7d = data_read_bytes_7d / 7
    daily_avg_14d = data_read_bytes_14d / 14

    # Use the 14-day average for longer periods (with a warning)
    print("\nNote: Historical data is limited to 14 days. Using 14-day average for longer periods.")

    # Compute Storage Tiers
    if storage_metrics['user_data']:
        # Frequent Access: Data accessed in last 7 days
        frequent_access = min(100, (data_read_bytes_7d / storage_metrics['user_data'] * 100) if storage_metrics['user_data'] > 0 else 0)
        
        # Infrequent Access: Data accessed between 7-14 days
        infrequent_access = min(100 - frequent_access,
                               ((data_read_bytes_14d - data_read_bytes_7d) / storage_metrics['user_data'] * 100) if storage_metrics['user_data'] > 0 else 0)
        
        # Deep Archive: Remaining data (since we can't determine older access patterns)
        deep_archive = max(0, 100 - frequent_access - infrequent_access)
        
        # Set Archive tiers to 0 since we don't have enough historical data
        archive_instant = 0
        archive = 0
    else:
        frequent_access = infrequent_access = archive_instant = archive = deep_archive = 0

    avg_object_size_mb = (storage_metrics['user_data'] * 1024) / files_used if files_used else 16  # Default to 16MB

    # Convert hourly operations to monthly
    monthly_write_ops = write_ops * HOURS_PER_MONTH
    monthly_read_ops = read_ops * HOURS_PER_MONTH
    monthly_metadata_ops = metadata_ops * DAYS_PER_MONTH

    # Print Results
    print("=" * 80)
    print("\n📊 FSxN Volume Metrics for S3 Intelligent-Tiering Cost Estimation 📊")
    print("=" * 80)

    # Volume Information
    print("\n🔹 Volume Information:")
    print(f"   FSxN File System ID: {args.fsx_id}")
    print(f"   FSxN Volume ID: {args.volume_id}")
    print(f"   Region: {args.region}")

    # Storage Information
    print("\n📦 Storage Information:")
    print(f"   Total Storage Used: {storage_metrics['user_data']:,.2f} GB")
    print(f"   Average Object Size: {avg_object_size_mb:.2f} MB")

    # Storage Tiers
    print("\n📊 Storage Tiers (based on available 14-day history):")
    print(f"   - Frequent Access: {frequent_access:.2f}%")
    print(f"   - Infrequent Access: {infrequent_access:.2f}%")
    print(f"   - Deep Archive Access: {deep_archive:.2f}%")

    # Access Patterns
    print("\n📊 Access Patterns (based on available 14-day history):")
    print(f"   - Last 7 Days: {daily_avg_7d:.2f} GB/day")
    print(f"   - Last 14 Days: {daily_avg_14d:.2f} GB/day")

    # Operations
    print("\nOperation Counts (based on hourly metrics, projected monthly):")
    print(f"   PUT, COPY, POST, LIST Requests: {monthly_write_ops:,.0f}")
    print(f"   GET, SELECT, and Other Read Requests: {monthly_read_ops:,.0f}")
    print(f"   Lifecycle Transitions: {monthly_metadata_ops:,.0f}")

    # S3 Select
    print("\n📊 S3 Select Usage (projected monthly based on 14-day history):")
    print(f"   Data Scanned: {monthly_data_scanned:.2f} GB/month")
    print(f"   Data Returned: {monthly_data_returned:.2f} GB/month (estimated as 30% of scanned data)")

    print("\n" + "=" * 80)
    print("\n✅ Use these values in the AWS Pricing Calculator for S3 Intelligent-Tiering!")
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()

