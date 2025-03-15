"""
OpenAI ChatGPT implementation of the sentiment provider.
"""

import os
import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

import boto3
import openai
from botocore.exceptions import ClientError
from model.post import Post
from providers.sentiment_provider import SentimentProvider
from supabase import create_client, Client

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Constants
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
JOBS_TABLE_NAME = os.environ["JOBS_TABLE_NAME"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Configure OpenAI client
openai.api_key = OPENAI_API_KEY


class ChatGPTProvider(SentimentProvider):
    """OpenAI ChatGPT implementation of the sentiment provider."""

    def get_provider_name(self) -> str:
        return "chatgpt"

    def create_sentiment_job(self, posts: List[Post], job_name: str) -> Dict[str, Any]:
        """
        Create a ChatGPT sentiment analysis job for a batch of posts.

        Args:
            posts: List of Post objects to analyze
            job_name: Name for the job

        Returns:
            Dict containing job information (job_id, job_name, etc.)
        """
        if not posts:
            return {"error": "No posts to analyze"}

        if not OPENAI_API_KEY:
            return {"error": "OpenAI API key not configured"}

        batch_id = self._create_batch_job(posts, job_name)
        job_id = str(uuid.uuid4())

        # Store the posts in S3 for reference
        input_key = f"chatgpt-jobs/input/{job_name}.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=input_key,
            Body=json.dumps([post.to_json() for post in posts]),
            ContentType="application/json",
        )

        # Store job metadata in DynamoDB
        jobs_table.put_item(
            Item={
                "job_id": job_id,
                "job_name": job_name,
                "status": "SUBMITTED",
                "created_at": datetime.now().isoformat(),
                "posts": [post.to_json() for post in posts],
                "provider": self.get_provider_name(),
                "openai_batch_id": batch_id,
                "version": 0,  # Initial version for optimistic locking
            }
        )

        logger.info(
            "Started ChatGPT sentiment analysis job %s (OpenAI batch ID: %s)",
            job_id,
            batch_id,
        )

        return {
            "job_id": job_id,
            "job_name": job_name,
            "provider": self.get_provider_name(),
            "status": "SUBMITTED",
            "openai_batch_id": batch_id,
        }

    def _create_batch_job(self, posts: List[Post], job_name: str) -> str:
        """
        Create a batch job with OpenAI.

        Args:
            posts: List of Post objects to analyze
            job_name: Name for the job

        Returns:
            batch_id
        """
        # Create a temporary file with JSONL format for batch API
        temp_file_path = f"/tmp/{job_name}.jsonl"

        with open(temp_file_path, "w", encoding="utf-8") as f:
            for post in posts:
                # Include a custom_id to match results later
                batch_item = {
                    "custom_id": post.id,
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": "gpt-3.5-turbo",
                        "messages": [
                            {
                                "role": "system",
                                "content": (
                                    "You are a sentiment analysis tool. "
                                    "Analyze the sentiment of the following text"
                                    "and respond with ONLY a JSON object with the format: "
                                    "{"
                                    "sentiment: POSITIVE|NEGATIVE|NEUTRAL|MIXED",
                                    "scores: {"
                                    "Positive: float,"
                                    "Negative: float,"
                                    "Neutral: float,"
                                    "Mixed: float"
                                    "}"
                                    "}"
                                    "The scores should sum to 1.0.",
                                ),
                            },
                            {"role": "user", "content": post.get_text()},
                        ],
                        "temperature": 0.3,
                        "max_tokens": 1000,
                    },
                }
                f.write(json.dumps(batch_item) + "\n")

        with open(temp_file_path, "rb") as f:
            file_response = openai.files.create(file=f, purpose="batch")

        batch_response = openai.batches.create(
            input_file_id=file_response.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        if batch_response.errors is not None:
            logger.error("Error creating batch job: %s", batch_response.errors)
            raise ValueError("Error creating batch job")

        batch_id = batch_response.id
        logger.info("Submitted batch job with ID: %s", batch_id)

        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

        return batch_id

    def check_job_status(self, job: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        """
        Check the status of a ChatGPT batch job.

        Args:
            job: Job metadata from DynamoDB

        Returns:
            Tuple of (status, output_file_id) where output_file_id is the OpenAI output file ID
        """
        batch_id = job["openai_batch_id"]

        batch_response = openai.batches.retrieve(batch_id)

        # Map OpenAI status to our status
        openai_status = batch_response.status
        if openai_status == "completed":
            return "COMPLETED", batch_response.output_file_id
        elif openai_status in ["failed", "cancelled", "expired"]:
            logger.error("OpenAI batch job %s: %s", openai_status, batch_response)
            return "FAILED", None
        elif openai_status == "in_progress":
            return "IN_PROGRESS", None
        else:
            return "SUBMITTED", None

    def process_completed_job(
        self, job: Dict[str, Any], output_file_id: str, current_version: int
    ) -> None:
        """
        Process a completed ChatGPT batch job.

        Args:
            job: Job metadata from DynamoDB
            output_file_id: ID of the output file from OpenAI
            current_version: Current version of the job record for optimistic locking
        """
        job_id = job["job_id"]
        posts = [Post.from_json(post) for post in job["posts"]]

        if not output_file_id:
            logger.error("No output file ID found for job %s", job_id)
            self._update_job_status(job_id, "FAILED", current_version)
            raise ValueError("No output file ID found")

        logger.info("Processing completed ChatGPT job %s", job_id)
        # Download results from OpenAI
        content = openai.files.content(output_file_id)
        content_str = content.read().decode("utf-8")

        results_to_store = []
        lines = content_str.strip().split("\n")

        for line in lines:
            logger.debug("Processing line: %s", line)
            openai_result = json.loads(line)
            if openai_result["response"]["status_code"] != 200:
                logger.warning("Unexpected response: %s", openai_result["response"])
                continue

            post_id = openai_result["custom_id"]
            if post_id not in [post.id for post in posts]:
                logger.warning("Post ID %s not found in job posts", post_id)
                continue

            # Find the corresponding post
            post = next(post for post in posts if post.id == post_id)

            # Extract sentiment data
            content = openai_result["response"]["body"]["choices"][0]["message"][
                "content"
            ]
            parsed = json.loads(content)
            sentiment = parsed["sentiment"]
            scores = parsed["scores"]

            # Prepare result data
            result_data = {
                "keyword": post.keyword,
                "created_time": post.created_at.isoformat(),
                "source": post.source,
                "post_id": post.id,
                "post_url": post.post_url,
                "sentiment": sentiment,
                "sentiment_scores": scores,
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
