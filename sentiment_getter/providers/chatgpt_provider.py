"""
OpenAI ChatGPT implementation of the sentiment provider.
"""

import os
import json
import logging
import uuid
from datetime import datetime

import openai
import boto3
from model.job import ChatGPTProviderData, Job
from model.post import Post
from model.sentiment import Sentiment
from providers.sentiment_provider import SentimentProvider

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ChatGPTProvider(SentimentProvider):
    """OpenAI ChatGPT implementation of the sentiment provider."""

    def get_provider_name(self) -> str:
        return "chatgpt"

    def create_sentiment_job(self, posts: list[Post], job_name: str) -> Job:
        if not posts:
            raise ValueError("No posts to analyze")

        openai.api_key = os.environ["OPENAI_API_KEY"]

        return self._create_batch_job(posts, job_name)

    def _create_batch_job(self, posts: list[Post], job_name: str) -> Job:
        # Create a temporary file with JSONL format for batch API
        temp_file_path = f"/tmp/{job_name}.jsonl"
        job_id = str(uuid.uuid4())

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
                                    "Analyze the sentiment of the following text "
                                    "and respond with ONLY a JSON object with the format: "
                                    '{"sentiment": "POSITIVE|NEGATIVE|NEUTRAL|MIXED", '
                                    '"scores": {'
                                    '"Positive": float, '
                                    '"Negative": float, '
                                    '"Neutral": float, '
                                    '"Mixed": float'
                                    "}}"
                                    " The scores should sum to 1.0."
                                ),
                            },
                            {"role": "user", "content": post.get_text()},
                        ],
                        "temperature": 0.3,
                        "max_tokens": 1000,
                    },
                }
                f.write(json.dumps(batch_item) + "\n")

        with open(temp_file_path, "rb") as file:
            file_content = file.read()  # Read content once
            file_response = openai.files.create(
                file=file_content,
                purpose="batch",
            )
            # save file to s3
            # s3 = boto3.client("s3")
            # s3.put_object(
            #     Bucket=os.environ["S3_BUCKET_NAME"],
            #     Key=f"{job_id}/input.jsonl",  # More descriptive filename
            #     Body=file_content,
            #     ContentType="text/plain",
            # )

        batch_response = openai.batches.create(
            input_file_id=file_response.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        if batch_response.errors is not None:
            logger.error("Error creating batch job: %s", batch_response.errors)
            raise ValueError("Error creating batch job")

        batch_id = batch_response.id
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

        return Job(
            job_id=job_id,
            job_name=job_name,
            status="SUBMITTED",
            created_at=datetime.now().isoformat(),
            posts=posts,
            provider=self.get_provider_name(),
            provider_data=ChatGPTProviderData(openai_batch_id=batch_id),
        )

    def query_and_update_job(self, job: Job):
        batch_id = job.provider_data.openai_batch_id

        batch_response = openai.batches.retrieve(batch_id)

        # Map OpenAI status to our status
        openai_status = batch_response.status
        if openai_status == "completed":
            # if batch_response is json, log it
            logger.info(
                "OpenAI batch job completed, batch_response: %s",
                batch_response,
            )
            # Update the job with the output file ID
            job.provider_data = ChatGPTProviderData(
                openai_batch_id=job.provider_data.openai_batch_id,
                output_file_id=batch_response.output_file_id,
                error_file_id=batch_response.error_file_id,
            )
            job.status = "COMPLETED"
            return True
        if openai_status in ["failed", "cancelling", "cancelled", "expired"]:
            logger.error("OpenAI batch job %s: %s", openai_status, batch_response)
            job.status = "FAILED"
            return False
        if openai_status == "in_progress":
            job.status = "IN_PROGRESS"
            return False
        return False

    def process_completed_job(self, job: Job) -> list[Sentiment]:
        if job.provider_data.error_file_id:
            content = openai.files.content(job.provider_data.error_file_id)
            # download error file to s3
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=os.environ["S3_BUCKET_NAME"],
                Key=f"{job.job_id}/error.jsonl",
                Body=content.text,
                ContentType="text/plain",
            )
            logger.warning(
                "Error file uploaded to s3: %s",
                f"{job.job_id}/error.jsonl",
            )
            return []

        if not job.provider_data.output_file_id:
            logger.error(
                "output_file_id is not set for job: %s\n"
                "make sure to call `openai.batches.retrieve` before",
                job.job_id,
            )
            return []

        content = openai.files.content(job.provider_data.output_file_id)
        content_str = content.read().decode("utf-8")
        logger.debug("Content: %s", content_str)

        sentiments: list[Sentiment] = []
        lines = content_str.strip().split("\n")

        for line in lines:
            logger.debug("Processing line: %s", line)
            openai_result = json.loads(line)
            if openai_result["response"]["status_code"] != 200:
                logger.warning("Unexpected response: %s", openai_result["response"])
                continue

            post_id = openai_result["custom_id"]
            if post_id not in [post.id for post in job.posts]:
                logger.warning("Post ID %s not found in job posts", post_id)
                continue

            # Find the corresponding post
            post = next(post for post in job.posts if post.id == post_id)
            logger.debug("Found matching post: %s", post)

            # Extract sentiment data
            content = openai_result["response"]["body"]["choices"][0]["message"][
                "content"
            ]
            parsed = json.loads(content)
            sentiment = parsed["sentiment"]
            scores = parsed["scores"]
            sentiments.append(
                Sentiment(
                    job=job,
                    post=post,
                    sentiment=sentiment,
                    mixed=scores["Mixed"],
                    positive=scores["Positive"],
                    negative=scores["Negative"],
                    neutral=scores["Neutral"],
                )
            )

        return sentiments
