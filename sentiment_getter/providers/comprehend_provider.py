"""
AWS Comprehend implementation of the sentiment provider.
"""

import os
import json
import io
import tarfile
from datetime import datetime

import boto3
from model.job import Job
from model.post import Post
from model.sentiment import Sentiment
from providers.sentiment_provider import SentimentProvider


class ComprehendProvider(SentimentProvider):
    """AWS Comprehend implementation of the sentiment provider."""

    def get_provider_name(self) -> str:
        return "comprehend"

    def create_sentiment_job(self, posts: list[Post], job_name: str, execution_id: str) -> Job:
        if not posts:
            return {"error": "No posts to analyze"}

        # Create a single input file with one post per line
        input_key = f"comprehend/jobs/input/{job_name}.txt"
        posts_text = "\n".join(post.get_text() for post in posts)
        bucket_name = os.environ["S3_BUCKET_NAME"]

        s3 = boto3.client("s3")
        comprehend = boto3.client("comprehend")

        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=input_key,
            Body=posts_text,
            ContentType="text/plain",
        )

        # Start Comprehend job
        response = comprehend.start_sentiment_detection_job(
            InputDataConfig={
                "S3Uri": f"s3://{bucket_name}/{input_key}",
                "InputFormat": "ONE_DOC_PER_LINE",
            },
            OutputDataConfig={"S3Uri": f"s3://{bucket_name}/comprehend/jobs/output/"},
            DataAccessRoleArn=os.environ["COMPREHEND_ROLE_ARN"],
            JobName=job_name,
            LanguageCode="en",
        )

        job = Job(
            job_id=response["JobId"],
            job_name=job_name,
            status="SUBMITTED",
            created_at=datetime.now(),
            post_keys=[post.get_s3_key() for post in posts],
            provider=self.get_provider_name(),
            logger=self.logger,
            execution_id=execution_id,
        )

        return job

    def query_and_update_job(self, job: Job) -> Job:
        comprehend = boto3.client("comprehend")
        job_details = comprehend.describe_sentiment_detection_job(JobId=job.job_id)
        # SUBMITTED | IN_PROGRESS | COMPLETED | FAILED | STOP_REQUESTED | STOPPED
        status = job_details["SentimentDetectionJobProperties"]["JobStatus"]
        if status == "COMPLETED":
            job.status = "COMPLETED"
            return job
        if status in ["STOP_REQUESTED", "STOPPED", "FAILED"]:
            job.status = "FAILED"
            return job
        if status == "IN_PROGRESS":
            job.status = "IN_PROGRESS"
            return job
        return job

    def process_completed_job(self, job: Job, posts: list[Post]) -> list[Sentiment]:
        s3 = boto3.client("s3")
        comprehend = boto3.client("comprehend")
        # Get job details from Comprehend
        job_details = comprehend.describe_sentiment_detection_job(JobId=job.job_id)
        output_s3_uri = job_details["SentimentDetectionJobProperties"][
            "OutputDataConfig"
        ]["S3Uri"]
        bucket_name = os.environ["S3_BUCKET_NAME"]

        # Extract the output key from the S3 URI
        output_key = output_s3_uri.replace(f"s3://{bucket_name}/", "")
        self.logger.info("Retrieving output file from: %s", output_key)

        # Get output file
        response = s3.get_object(Bucket=bucket_name, Key=output_key)
        self.logger.info("Successfully retrieved output file")

        # Extract and process results
        with tarfile.open(
            fileobj=io.BytesIO(response["Body"].read()), mode="r:gz"
        ) as tar:
            # Get output files and filter for valid ones
            files = filter(
                None,
                [
                    tar.extractfile(member)
                    for member in tar.getmembers()
                    if member.name == "output"
                ],
            )

            # Process each file's lines (each line is a processed post) into sentiments
            # Assumming that they are in the same order as the posts
            # Edit: there is a `batch_detect_sentiment` from boto3, I am stupid
            sentiments = []
            for f in files:
                lines = f.read().decode("utf-8").strip().split("\n")

                for line_index, line_content in enumerate(lines):
                    sentiment_response = json.loads(line_content)
                    sentiments.append(
                        Sentiment(
                            job=job,
                            post=posts[line_index],
                            sentiment=sentiment_response["Sentiment"],
                            mixed=sentiment_response["SentimentScore"]["Mixed"],
                            positive=sentiment_response["SentimentScore"]["Positive"],
                            negative=sentiment_response["SentimentScore"]["Negative"],
                            neutral=sentiment_response["SentimentScore"]["Neutral"],
                        )
                    )

        return sentiments
