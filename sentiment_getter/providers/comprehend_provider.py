"""
AWS Comprehend implementation of the sentiment provider.
"""

import os
import logging
import json
import io
import tarfile
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

import boto3
from botocore.exceptions import ClientError
from model.post import Post
from providers.sentiment_provider import SentimentProvider
from supabase import create_client, Client

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
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


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

        logger.info(
            "Started AWS Comprehend sentiment analysis job %s", response["JobId"]
        )

        return {
            "job_id": response["JobId"],
            "job_name": job_name,
            "provider": self.get_provider_name(),
            "status": "SUBMITTED",
        }

    def check_job_status(self, job: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        """
        Check the status of a Comprehend job.

        Args:
            job: Job metadata from DynamoDB

        Returns:
            Tuple of (status, output_file_id) where output_file_id is None for Comprehend
        """
        job_id = job["job_id"]
        job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
        status = job_details["SentimentDetectionJobProperties"]["JobStatus"]
        return status, None

    def process_completed_job(
        self, job: Dict[str, Any], output_file_id: str, current_version: int
    ) -> None:
        """
        Process a completed Comprehend job.

        Args:
            job: Job metadata from DynamoDB
            output_file_id: Not used for Comprehend
            current_version: Current version of the job record for optimistic locking
        """
        job_id = job["job_id"]
        posts = job["posts"]

        logger.info("Processing completed Comprehend job %s", job_id)
        # Get job details from Comprehend
        job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
        output_s3_uri = job_details["SentimentDetectionJobProperties"][
            "OutputDataConfig"
        ]["S3Uri"]

        # Extract the output key from the S3 URI
        output_key = output_s3_uri.replace(f"s3://{BUCKET_NAME}/", "")
        logger.info("Retrieving output file from: %s", output_key)

        # Get output file
        response = s3.get_object(Bucket=BUCKET_NAME, Key=output_key)
        tar_content = response["Body"].read()
        logger.info("Successfully retrieved output file")

        # Extract and process results
        results_to_store = []
        with tarfile.open(fileobj=io.BytesIO(tar_content), mode="r:gz") as tar:
            for member in tar.getmembers():
                if member.name == "output":
                    f = tar.extractfile(member)
                    if f:
                        content = f.read().decode("utf-8")
                        lines = content.strip().split("\n")

                        # Process each line (one result per line)
                        for line_number, line in enumerate(lines):
                            result = json.loads(line)

                            # Find the post metadata for the result, which is from the line number
                            post = Post.from_json(posts[line_number])

                            # Prepare result data
                            result_data = {
                                "keyword": post.keyword,
                                "created_time": post.created_at.isoformat(),
                                "source": post.source,
                                "post_id": post.id,
                                "post_url": post.post_url,
                                "sentiment": result["Sentiment"],
                                "sentiment_scores": result["SentimentScore"],
                                "job_id": job_id,
                            }
                            results_to_store.append(result_data)

        self._store_results_in_supabase(results_to_store)
        self._update_job_status(job_id, "PROCESSED", current_version)

    def _store_results_in_supabase(self, results):
        """Store sentiment results in Supabase using upsert to handle duplicates"""
        if not results:
            return

        records = [
            {
                "keyword": result["keyword"],
                "created_time": result["created_time"],
                "source": result["source"],
                "post_id": result["post_id"],
                "post_url": result["post_url"],
                "sentiment": result["sentiment"],
                "sentiment_score_mixed": float(result["sentiment_scores"]["Mixed"]),
                "sentiment_score_positive": float(
                    result["sentiment_scores"]["Positive"]
                ),
                "sentiment_score_neutral": float(result["sentiment_scores"]["Neutral"]),
                "sentiment_score_negative": float(
                    result["sentiment_scores"]["Negative"]
                ),
                "job_id": result["job_id"],
            }
            for result in results
        ]

        data = (
            supabase.table("sentiment_results")
            .upsert(records, on_conflict="post_id,job_id")
            .execute()
        )
        if data.count is not None:
            logger.info("Successfully upserted %d records in Supabase", data.count)
        return data

    def _update_job_status(
        self, job_id: str, status: str, version: int
    ) -> Tuple[bool, int]:
        """Update job status with optimistic locking"""
        try:
            # Calculate TTL for 30 days from now
            ttl = int((datetime.now() + timedelta(days=30)).timestamp())

            response = jobs_table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #status = :status, version = :version, #ttl = :ttl",
                ConditionExpression="version = :current_version",
                ExpressionAttributeNames={"#status": "status", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":status": status,
                    ":version": version + 1,
                    ":current_version": version,
                    ":ttl": ttl,
                },
                ReturnValues="ALL_NEW",
            )
            return True, response["Attributes"]["version"]
        except ClientError as e:
            logger.error("Error updating job %s: %s", job_id, str(e))
            return False, version
