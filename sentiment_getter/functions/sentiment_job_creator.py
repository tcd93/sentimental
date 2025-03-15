"""
Lambda function to create Comprehend sentiment analysis jobs from batches of posts.
"""

import json
import os
import logging
from datetime import datetime
import boto3
from model.post import Post

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client("s3")
comprehend = boto3.client("comprehend")
dynamodb = boto3.resource("dynamodb")

# Constants
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
JOBS_TABLE_NAME = os.environ["JOBS_TABLE_NAME"]
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)


def lambda_handler(event, _):
    """
    Create a Comprehend sentiment analysis job for posts.

    Args:
        event: Array of sources, each with a list of Post objects

    Returns:
        Dict containing job information
    """
    logger.info(
        "Creating sentiment analysis job with parameters: %s", json.dumps(event)
    )

    # event must be a list
    if not isinstance(event, list):
        raise ValueError("Invalid input format: event must be a list")
    # each element in event must be a list
    for source in event:
        if not isinstance(source, list):
            raise ValueError(
                "Invalid input format: each source in event must be a list"
            )

    # flatten and convert to list of serialized Post objects
    posts: list[Post] = [Post.from_json(post) for source in event for post in source]

    if not posts or len(posts) == 0:
        return {"statusCode": 404, "body": json.dumps("No posts to analyze")}

    # Create a single input file with one post per line
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_name = f"job_{timestamp}"
    input_key = f"comprehend-jobs/input/{job_name}.txt"
    posts_text = "\n".join(post.get_text() for post in posts)

    # Upload to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=input_key,
        Body=posts_text.encode("utf-8"),
        ContentType="text/plain",
    )

    # Start Comprehend job
    response = comprehend.start_sentiment_detection_job(
        InputDataConfig={
            "S3Uri": f"s3://{BUCKET_NAME}/{input_key}",
            "InputFormat": "ONE_DOC_PER_LINE",
        },
        OutputDataConfig={"S3Uri": f"s3://{BUCKET_NAME}/comprehend-jobs/output/"},
        DataAccessRoleArn=os.environ["COMPREHEND_ROLE_ARN"],
        JobName=job_name,
        LanguageCode="en",
    )

    # Store job metadata in DynamoDB
    jobs_table.put_item(
        Item={
            "job_id": response["JobId"],
            "job_name": job_name,
            "status": "SUBMITTED",
            "created_at": datetime.now().isoformat(),
            "posts": [post.to_json() for post in posts],
            "version": 0,  # Initial version for optimistic locking
        }
    )

    logger.info("Started sentiment analysis job %s", response["JobId"])

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": f"Started sentiment analysis for {len(posts)} posts",
                "job_id": response["JobId"],
                "job_name": job_name,
            }
        ),
    }
