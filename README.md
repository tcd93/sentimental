# Sentiment Analysis Pipeline

This project is a serverless sentiment analysis data pipeline that collects posts from Reddit and Steam game reviews, analyzes sentiment, and stores results in AWS.

## Architecture

- **Data Sources**: Reddit posts and Steam game reviews
- **Pipeline**: AWS Step Functions orchestrating Lambda functions
- **Storage**: Kinesis Firehose → S3 → Iceberg tables in AWS Glue
- **Schedule**: Runs every 12 hours via EventBridge Scheduler
- **Final Output**: `sentiment` table in `sentimental` Glue database

## Prerequisites

- AWS CLI configured with appropriate credentials
- AWS SAM CLI installed
- Python 3.12+
- Required Python packages: `tomli`, `boto3`
- An S3 bucket for deployment artifacts
- Reddit API credentials (Client ID and Secret)
- OpenAI API key (for sentiment analysis)

## Deployment

### 1. Configure AWS Profile

Ensure your AWS credentials are configured:
```bash
aws configure --profile <your-profile-name>
```

### 2. Create Configuration File

Copy the example configuration and fill in your parameters:
```bash
cp samconfig.toml.example samconfig.toml
```

Edit `samconfig.toml` with your values:
- `BucketName`: S3 bucket for storing data
- `RedditClientId`: Your Reddit API client ID
- `RedditClientSecret`: Your Reddit API client secret
- `OpenAIApiKey`: Your OpenAI API key (optional)
- AWS profile and region settings

### 3. Create Iceberg Tables

Before deploying the SAM template, create the required Iceberg tables in Athena:
```bash
cd sentiment_getter/scripts
python create_iceberg_table.py
cd ../..
```

This script creates two tables:
- `post`: Stores raw posts from Reddit and Steam
- `sentiment`: Stores sentiment analysis results

### 4. Build and Deploy SAM Template

Build the SAM application:
```bash
sam build
```

Deploy to AWS:
```bash
sam deploy
```

For first-time deployment, use guided mode:
```bash
sam deploy --guided
```

### 5. Upload Keywords Configuration

Create a keywords configuration file at `s3://<your-bucket>/config/keywords_config.json`:
```json
{
  "source": {
    "reddit": [
      {
        "keyword": "example_game",
        "subreddits": ["gaming"],
        "sort": "top",
        "time_filter": "day",
        "post_limit": 10
      }
    ],
    "steam": [
      {
        "keyword": "example_game",
        "sort": "top",
        "post_limit": 10
      }
    ]
  }
}
```

## Pipeline Configuration

### Schedule
The pipeline is configured to run every 12 hours. To modify the schedule, edit the `ScheduleExpression` in `template.yaml`:
```yaml
ScheduleExpression: rate(12 hours)  # or use cron: cron(0 0,12 * * ? *)
```

To disable automatic execution, set `State: DISABLED` in the template.

### Manual Execution

To manually trigger the pipeline:
```bash
aws stepfunctions start-execution \
  --state-machine-arn <KeywordAPIPipelineArn> \
  --input '{}'
```

The ARN is available in the stack outputs after deployment.

## Monitoring

- **CloudWatch Logs**: All Lambda functions and Step Functions log to CloudWatch
- **Step Functions Console**: Monitor pipeline executions and view execution history
- **Athena**: Query the `sentiment` table for analysis results
- **SNS Notifications**: Error notifications sent to the configured SNS topic

## Data Access

Query sentiment data using Athena:
```sql
SELECT * FROM sentimental.sentiment
WHERE created_at >= current_date - interval '7' day
ORDER BY created_at DESC;
```


