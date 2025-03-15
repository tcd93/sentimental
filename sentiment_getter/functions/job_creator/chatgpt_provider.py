"""
OpenAI ChatGPT implementation of the sentiment provider.
"""

import os
import json
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Any

import boto3
import openai
from model.post import Post
from functions.job_creator.sentiment_provider import SentimentProvider

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Constants
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
JOBS_TABLE_NAME = os.environ["JOBS_TABLE_NAME"]
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)

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

        # Create a batch job with OpenAI
        batch_id, file_id = self._create_batch_job(posts, job_name)
        if not batch_id:
            raise ValueError("Failed to create batch job, check logs for more details")

        # Generate a unique job ID for our system
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
                "openai_file_id": file_id,
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

    def _create_batch_job(self, posts: List[Post], job_name: str) -> tuple[str, str]:
        """
        Create a batch job with OpenAI.

        Args:
            posts: List of Post objects to analyze
            job_name: Name for the job

        Returns:
            Tuple of (batch_id, file_id) or ("", "") if failed
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
                                    "The scores should sum to 1.0."
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

        batch_id = batch_response.id

        logger.info("Submitted batch job with ID: %s", batch_id)

        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

        return batch_id, file_response.id
