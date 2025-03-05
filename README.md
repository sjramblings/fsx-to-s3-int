# FSx to S3 Intelligent-Tiering Cost Estimation Tool

This tool helps AWS users analyze their FSx for NetApp ONTAP (FSxN) volume usage patterns to estimate potential cost savings when migrating to Amazon S3 Intelligent-Tiering. It collects and analyzes metrics such as storage usage, access patterns, and operations to provide insights for data migration planning.

## Features

- 📊 Detailed storage metrics analysis
- 📈 Access pattern analysis for intelligent tiering estimation
- 🔄 Throughput and operations monitoring
- 💰 Cost optimization insights
- 📋 Comprehensive reporting of volume statistics

## Prerequisites

- Python 3.6 or higher
- AWS credentials configured with appropriate permissions
- Access to FSx for NetApp ONTAP volumes
- Required permissions for CloudWatch metrics

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/fsx-to-s3-int.git
cd fsx-to-s3-int
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Ensure your AWS credentials are properly configured. You can do this by:
- Using AWS CLI: `aws configure --profile your-profile-name`
- Setting environment variables
- Using IAM roles if running on AWS infrastructure

## Usage

Run the script with the following required parameters:

```bash
python fsx_to_s3_int.py --fsx-id fs-xxxxxxxxxxxxxxxxx \
                        --volume-id fsvol-xxxxxxxxxxxxxxxxx \
                        --region your-aws-region \
                        --profile your-aws-profile
```

### Parameters

- `--fsx-id`: Your FSx File System ID
- `--volume-id`: Your FSx Volume ID
- `--region`: AWS Region where your FSx system is located
- `--profile`: AWS CLI profile to use for authentication

## Output

The tool provides detailed metrics including:
- Total storage usage and capacity
- File count and average object size
- Access patterns for different time periods
- Estimated distribution across S3 Intelligent-Tiering access tiers
- Monthly operation counts (read, write, metadata)

## Metrics Collected

- Storage metrics (capacity, usage, available space)
- Data access patterns (7-day and 14-day periods)
- Operation counts (read, write, metadata)
- File system statistics
- Throughput measurements

## Limitations

- Historical data is limited to 14 days due to CloudWatch metrics retention
- Access pattern analysis is based on available CloudWatch metrics
- Estimates are projections and actual results may vary

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.

## Support

For issues, questions, or contributions, please open an issue in the GitHub repository. 