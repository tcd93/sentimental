"""
Model for sentiment analysis jobs stored in DynamoDB.
"""

import json
import os
from dataclasses import dataclass
import logging
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError
from model.post import Post

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource("dynamodb")

# Constants
JOBS_TABLE_NAME = os.environ.get("JOBS_TABLE_NAME", "")
jobs_table = dynamodb.Table(JOBS_TABLE_NAME) if JOBS_TABLE_NAME else None


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


class Job:
    """Represents a sentiment analysis job."""

    def __init__(
        self,
        job_id: str,
        job_name: str,
        status: str,
        created_at: datetime,
        posts: list[Post],
        provider: str,
        provider_data: ChatGPTProviderData | ComprehendProviderData = None,
    ):
        """
        Initialize a Job object.

        Args:
            job_id: Unique identifier for the job
            job_name: Human-readable name for the job
            status: Current status of the job (SUBMITTED, IN_PROGRESS, COMPLETED, etc.)
            created_at: Timestamp when the job was created
            posts: List of posts to analyze
            provider: Name of the sentiment provider (comprehend, chatgpt)
            provider_data: Provider-specific data
                           (e.g. openai_batch_id, output_file_id from ChatGPT)
        """
        self.job_id = job_id
        self.job_name = job_name
        self.status = status
        self.created_at = (
            created_at
            if isinstance(created_at, datetime)
            else datetime.fromisoformat(created_at)
        )
        self.posts = posts
        self.provider = provider
        self.provider_data = provider_data

    @classmethod
    def from_dict(cls, data: dict[str, any]) -> "Job":
        """
        Create a Job object from a dictionary.

        Args:
            data: Dictionary representation of a Job
            provider: Sentiment provider
        """
        if data.get("provider") == "chatgpt":
            provider_data = ChatGPTProviderData.from_dict(data["provider_data"])
        elif data.get("provider") == "comprehend":
            provider_data = ComprehendProviderData.from_dict(data["provider_data"])
        else:
            raise ValueError(f"Unsupported provider: {data['provider']}")

        return cls(
            job_id=data["job_id"],
            job_name=data["job_name"],
            status=data["status"],
            created_at=data["created_at"],
            posts=[Post.from_dict(post) for post in data["posts"]],
            provider=data["provider"],
            provider_data=provider_data,
        )

    def to_dict(self) -> dict[str, any]:
        """
        Convert the Job object to a dictionary.

        Returns:
            Dictionary representation of the Job
        """
        result = {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "posts": [post.to_dict() for post in self.posts],
            "provider": self.provider,
            "provider_data": (
                self.provider_data.to_dict() if self.provider_data else None
            ),
        }

        return result

    def sync_dynamodb(self) -> bool:
        """
        Sync the job to DynamoDB. With optimistic locking.

        Returns:
            True if the job was synced, False otherwise
        """
        if not jobs_table:
            logger.error("JOBS_TABLE_NAME environment variable not set")
            return False

        try:
            response = jobs_table.put_item(
                Item=self.to_dict()
                # Add ttl to the item
                | {"ttl": int((datetime.now() + timedelta(days=30)).timestamp())},
                ReturnConsumedCapacity="TOTAL",
            )
            logger.info(
                "Synced job to DynamoDB, total capacity units consumed: %s",
                response.get("ConsumedCapacity", {}).get("CapacityUnits", "N/A"),
            )
            return True
        except ClientError as e:
            logger.error("Error syncing job to DynamoDB: %s", str(e))
            return False
