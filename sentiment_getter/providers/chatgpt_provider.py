"""
OpenAI ChatGPT implementation of the sentiment provider.
"""

import os
import json
import uuid
from datetime import datetime

import openai
import boto3
from model.job import ChatGPTProviderData, Job
from model.post import Post
from model.sentiment import Sentiment
from providers.sentiment_provider import SentimentProvider

class ChatGPTProvider(SentimentProvider):
    """OpenAI ChatGPT implementation of the sentiment provider."""

    def get_provider_name(self) -> str:
        return "chatgpt"

    def create_sentiment_job(self, posts: list[Post], job_name: str, execution_id: str) -> Job:
        if not posts:
            raise ValueError("No posts to analyze")

        openai.api_key = os.environ["OPENAI_API_KEY"]

        return self._create_batch_job(posts, job_name, execution_id)

    def _create_batch_job(self, posts: list[Post], job_name: str, execution_id: str) -> Job:
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
                        "model": "gpt-4o-mini",
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

        batch_response = openai.batches.create(
            input_file_id=file_response.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        if batch_response.errors is not None:
            self.logger.error("Error creating batch job: %s", batch_response.errors)
            raise ValueError("Error creating batch job")

        batch_id = batch_response.id
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

        return Job(
            job_id=job_id,
            job_name=job_name,
            status="SUBMITTED",
            created_at=datetime.now(),
            post_keys=[post.get_s3_key() for post in posts],
            provider=self.get_provider_name(),
            provider_data=ChatGPTProviderData(openai_batch_id=batch_id),
            logger=self.logger,
            execution_id=execution_id,
        )

    def query_and_update_job(self, job: Job) -> Job:
        batch_id = job.provider_data.openai_batch_id

        batch_response = openai.batches.retrieve(batch_id)

        # Map OpenAI status to our status
        openai_status = batch_response.status
        if openai_status == "completed":
            self.logger.info(
                "OpenAI batch job completed, batch_response: %s",
                batch_response.to_json(indent=2),
            )
            # Update the job with the output file ID
            job.provider_data = ChatGPTProviderData(
                openai_batch_id=job.provider_data.openai_batch_id,
                output_file_id=batch_response.output_file_id,
                error_file_id=batch_response.error_file_id,
            )
            job.status = "COMPLETED"
            return job
        if openai_status in ["failed", "cancelling", "cancelled", "expired"]:
            self.logger.error("OpenAI batch job %s: %s", openai_status, batch_response)
            job.status = "FAILED"
            return job
        if openai_status == "in_progress":
            job.status = "IN_PROGRESS"
            return job
        return job

    def process_completed_job(self, job: Job, posts: list[Post]) -> list[Sentiment]:
        if job.provider_data.error_file_id:
            content = openai.files.content(job.provider_data.error_file_id)
            # download error file to s3
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=os.environ["S3_BUCKET_NAME"],
                Key=f"chatgpt/jobs/{job.job_id}/error.jsonl",
                Body=content.text,
                ContentType="text/plain",
            )
            self.logger.warning(
                "Error file uploaded to s3: %s",
                f"jobs/{job.job_id}/error.jsonl",
            )
            return []

        if not job.provider_data.output_file_id:
            self.logger.error(
                "output_file_id is not set for job: %s\n"
                "make sure to call `openai.batches.retrieve` before",
                job.job_id,
            )
            return []

        content = openai.files.content(job.provider_data.output_file_id)
        self.logger.debug("Content: %s", content.text)

        sentiments: list[Sentiment] = []
        lines = content.text.strip().split("\n")

        for line in lines:
            self.logger.debug("Processing line: %s", line)
            openai_result = json.loads(line)
            if openai_result["response"]["status_code"] != 200:
                self.logger.warning("Unexpected response: %s", openai_result["response"])
                continue

            post_id = openai_result["custom_id"]
            if post_id not in [post.id for post in posts]:
                continue

            # Find the corresponding post
            post = next(post for post in posts if post.id == post_id)
            self.logger.debug("Found matching post: %s", post)

            # Extract sentiment data
            content = openai_result["response"]["body"]["choices"][0]["message"][
                "content"
            ]
            # Sometimes the bot will f-up and return a non-json object
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                self.logger.warning("Failed to parse content: %s", content)
                continue
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
