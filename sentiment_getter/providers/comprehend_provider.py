"""
AWS Comprehend implementation of the sentiment provider.
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Any

import boto3
from model.post import Post
from providers.sentiment_provider import SentimentProvider

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


class ComprehendProvider(SentimentProvider):
    """AWS Comprehend implementation of the sentiment provider."""

    def get_provider_name(self) -> str:
        return "comprehend"

    def create_sentiment_job(self, posts: List[Post], job_name: str) -> Dict[str, Any]:
        """
        Create a AWS Comprehend sentiment analysis job for a batch of posts.

        Args:
            posts: List of Post objects to analyze
            job_name: Name for the job

        Returns:
            Dict containing job information (job_id, job_name, etc.)
        """
        if not posts:
            return {"error": "No posts to analyze"}

        # Create a single input file with one post per line
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
                "provider": self.get_provider_name(),
                "version": 0,  # Initial version for optimistic locking
            }
        )

        logger.info("Started AWS Comprehend sentiment analysis job %s", response["JobId"])

        return {
            "job_id": response["JobId"],
            "job_name": job_name,
            "provider": self.get_provider_name(),
            "status": "SUBMITTED",
        }
