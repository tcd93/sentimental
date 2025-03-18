"""
Model for sentiment analysis jobs stored in DynamoDB.

Keep the size small to save cost:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html#write-operation-consumption
"""

import os
from dataclasses import dataclass
import logging
from datetime import datetime, timedelta

import boto3
from model.post import Post


@dataclass(frozen=True)
class ChatGPTProviderData:
    """Provider-specific data for ChatGPT."""

    openai_batch_id: str
    """The ID of the batch job created by OpenAI."""

    output_file_id: str | None = None
    """The ID of the output file created by OpenAI (when the job is completed)."""

    error_file_id: str | None = None
    """The ID of the error file created by OpenAI (when the job fails)."""

    def to_dict(self) -> dict[str, any]:
        """Convert the ChatGPTProviderData object to a dictionary."""
        return {
            "openai_batch_id": self.openai_batch_id,
            "output_file_id": self.output_file_id,
            "error_file_id": self.error_file_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, any]) -> "ChatGPTProviderData":
        """Convert a dictionary to a ChatGPTProviderData object."""
        return cls(
            openai_batch_id=data["openai_batch_id"],
            output_file_id=data.get("output_file_id"),
            error_file_id=data.get("error_file_id"),
        )


@dataclass(frozen=True)
class ComprehendProviderData:
    """Provider-specific data for Comprehend."""

    @classmethod
    def from_dict(cls, _: dict[str, any]) -> "ComprehendProviderData":
        """Convert a dictionary to a ComprehendProviderData object."""
        return cls()


@dataclass
class Job:
    """Represents a sentiment analysis job."""

    job_id: str
    job_name: str
    status: str
    created_at: datetime
    posts: list[Post]
    provider: str
    provider_data: ChatGPTProviderData | ComprehendProviderData = None
    logger: logging.Logger | None = None

    def __post_init__(self):
        if self.logger is None:
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)

        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)

    @classmethod
    def reconstruct(cls, data: dict[str, any], logger: logging.Logger = None) -> "Job":
        """
        Create a Job object from job metadata (exported from `to_dict_minimal`).
        """
        if data.get("provider") == "chatgpt":
            provider_data = ChatGPTProviderData.from_dict(data["provider_data"])
        elif data.get("provider") == "comprehend":
            provider_data = ComprehendProviderData.from_dict(data["provider_data"])
        else:
            raise ValueError(f"Unsupported provider: {data['provider']}")

        # if data["posts"] is a list of post ids (str), read posts from S3
        posts = []
        if (
            data["posts"]
            and isinstance(data["posts"], list)
            and isinstance(data["posts"][0], str)
        ):
            s3 = boto3.client("s3")
            start_time = datetime.now()
            for post_id in data["posts"]:
                response = s3.get_object(
                    Bucket=os.environ["S3_BUCKET_NAME"],
                    Key=f"chatgpt/posts/{post_id}.json",
                )
                post_json = response["Body"].read().decode("utf-8")
                posts.append(Post.from_json(post_json))
            end_time = datetime.now()
            if logger:
                logger.info(
                    "Read %s posts from S3. Total time (seconds): %s",
                    len(data["posts"]),
                    (end_time - start_time).total_seconds(),
                )
        else:
            posts = data["posts"]

        return cls(
            job_id=data["job_id"],
            job_name=data["job_name"],
            status=data["status"],
            created_at=data["created_at"],
            posts=posts,
            provider=data["provider"],
            provider_data=provider_data,
            logger=logger,
        )

    def to_dict_minimal(self) -> dict[str, any]:
        """
        Convert the Job object to a dictionary. Only keeping post id in "posts" field to save space.

        Returns:
            Dictionary representation of the Job
        """
        result = {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "status": self.status,
            "created_at": (
                self.created_at.isoformat()
                if isinstance(self.created_at, datetime)
                else self.created_at
            ),
            "posts": [post.id for post in self.posts],
            "provider": self.provider,
            "provider_data": (
                self.provider_data.to_dict() if self.provider_data else None
            ),
        }

        return result

    def persist(self):
        """
        Persist the Job metadata and posts info to database and S3.
        You might want to call `persist_main` instead.
        """
        self.logger.info("Persisting job ID: %s", self.job_id)
        self.persist_meta()
        self._persist_posts()

    def persist_meta(self):
        """
        Persist the Job info to database.
        """
        dynamodb = boto3.resource("dynamodb")
        jobs_table = dynamodb.Table(os.environ["JOBS_TABLE_NAME"])

        response = jobs_table.put_item(
            Item=self.to_dict_minimal()
            | {"ttl": int((datetime.now() + timedelta(days=30)).timestamp())},
            ReturnConsumedCapacity="TOTAL",
        )
        self.logger.info(
            "Synced job to DynamoDB, total capacity units consumed: %s",
            response.get("ConsumedCapacity", {}).get("CapacityUnits", "N/A"),
        )
        return True

    def _persist_posts(self):
        """
        Persist the posts to database (current: S3).
        """
        s3 = boto3.client("s3")
        bucket_name = os.environ["S3_BUCKET_NAME"]

        start_time = datetime.now()
        for post in self.posts:
            s3.put_object(
                Bucket=bucket_name,
                Key=f"chatgpt/posts/{post.id}.json",
                Body=post.to_json(),
                ContentType="application/json",
            )
        end_time = datetime.now()
        self.logger.info(
            "Persisted %s posts to S3. Total time (seconds): %s",
            len(self.posts),
            (end_time - start_time).total_seconds(),
        )
        return True
