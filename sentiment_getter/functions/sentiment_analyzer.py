"""
Lambda function to analyze sentiment of posts and send results to storage queue.
"""

import json
import os
import logging
import uuid
from datetime import datetime
import boto3
from model.post import Post
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")
comprehend = boto3.client("comprehend")
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
JOBS_TABLE_NAME = os.environ.get("JOBS_TABLE_NAME")
jobs_table = dynamodb.Table(JOBS_TABLE_NAME) if JOBS_TABLE_NAME else None
JOB_OUTPUT_PREFIX = "comprehend-jobs/output/"
JOB_INPUT_PREFIX = "comprehend-jobs/input/"


def start_sentiment_analysis_job(
    post: Post, keyword: str, source: str, job_name: str = None
):
    """
    Start an asynchronous sentiment analysis job using AWS Comprehend for a single post.

    Args:
        post: Post object to analyze
        job_name: Optional name for the job
        keyword: The keyword being analyzed
        source: The source of the data (e.g., "reddit")

    Returns:
        dict: Job information including job ID and input file location
    """
    comment_text = post.get_comment_text()
    if not comment_text.strip():
        logger.info("Post has no comments to analyze")
        return None

    # Create a unique job name if not provided
    if not job_name:
        # Include post ID in job name for better tracking
        job_name = f"sa-{post.id[:8]}-{str(uuid.uuid4())[:8]}"

    # Create input file for Comprehend job
    input_key = f"{JOB_INPUT_PREFIX}{job_name}.txt"

    # Upload input file to S3
    logger.info("Uploading input file to S3: %s", input_key)
    s3.put_object(
        Bucket=BUCKET_NAME, Key=input_key, Body=comment_text, ContentType="text/plain"
    )

    # Start Comprehend sentiment analysis job
    logger.info("Starting Comprehend sentiment analysis job: %s", job_name)
    response = comprehend.start_sentiment_detection_job(
        InputDataConfig={
            "S3Uri": f"s3://{BUCKET_NAME}/{input_key}",
            "InputFormat": "ONE_DOC_PER_LINE",
        },
        OutputDataConfig={"S3Uri": f"s3://{BUCKET_NAME}/{JOB_OUTPUT_PREFIX}"},
        DataAccessRoleArn=os.environ["COMPREHEND_ROLE_ARN"],
        JobName=job_name,
        LanguageCode="en",
    )

    job_id = response["JobId"]
    logger.info("Comprehend job started with ID: %s", job_id)

    # Store job information in DynamoDB if table is configured
    if jobs_table:
        try:
            jobs_table.put_item(
                Item={
                    "job_id": job_id,
                    "job_name": job_name,
                    "status": "SUBMITTED",
                    "created_at": datetime.now().isoformat(),
                    "keyword": keyword,
                    "source": source,
                    "post_metadata": {
                        "id": post.id,
                        "title": post.title,
                        "created_at": post.created_at.isoformat(),
                    },
                }
            )
            logger.info("Stored job information in DynamoDB for job %s", job_id)
        except ClientError as e:
            logger.error("DynamoDB service error storing job information: %s", e)
        except KeyError as e:
            logger.error("Missing key in job data when storing in DynamoDB: %s", e)
        except (TypeError, ValueError) as e:
            logger.error("Data format error when storing job in DynamoDB: %s", e)
    else:
        logger.warning("DynamoDB table not configured, job tracking will be limited")

    return {
        "job_id": job_id,
        "job_name": job_name,
    }


def lambda_handler(event, _):
    """
    Process posts from SQS and analyze sentiment.

    Args:
        event: SQS event containing posts
        _: Lambda context

    Returns:
        Dict containing status code and message
    """
    records = event.get("Records", [])
    logger.info("Starting sentiment analysis for %d messages", len(records))

    if not records:
        return {
            "statusCode": 200,
            "body": json.dumps("No records to process"),
        }

    # Process each post individually
    job_results = []
    for record in records:
        # Parse message
        data = json.loads(record["body"])
        post = Post.from_dict(data["post"])
        keyword = data["keyword"]
        source = data["source"]

        # Start a sentiment analysis job for this post
        job_info = start_sentiment_analysis_job(post, keyword=keyword, source=source)

        if job_info:
            job_results.append(job_info)
            logger.info("Started sentiment analysis job %s for post %s", job_info["job_id"], post.id)

    if not job_results:
        logger.info("No valid posts to analyze")
        return {
            "statusCode": 200,
            "body": json.dumps("No valid posts to analyze"),
        }

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": f"Started {len(job_results)} sentiment analysis jobs",
                "jobs": job_results,
            }
        ),
    }
