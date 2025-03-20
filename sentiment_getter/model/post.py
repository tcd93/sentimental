"""
This module contains the Post class, which is used to represent a post on a social media platform.
"""

from dataclasses import dataclass
from datetime import datetime
import logging
import json
import os
import boto3


@dataclass
# pylint: disable=too-many-instance-attributes
class Post:
    """Post object representing a social media post with comments."""

    id: str
    keyword: str
    source: str
    title: str
    created_at: datetime
    body: str
    comments: list[str]
    post_url: str = ""  # Optional URL to the original post
    logger: logging.Logger | None = None

    def __post_init__(self):
        if self.logger is None:
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)

    def get_text(self) -> str:
        """
        Get the text of the post and comments as a single line for Comprehend processing
        Words are limited to save cost and space
        """
        # Limit each comment to 300 words and join all text with spaces
        truncated_comments = " - ".join(
            [comment[:200].replace("\n", ".") for comment in self.comments]
        )
        return (
            f"title: {self.title.replace('\n', '.')}; "
            f"body: {self.body[:300].replace('\n', '.')}; "
            f"comments: {truncated_comments}"
        )

    def to_dict(self) -> dict:
        """Convert Post to dictionary for serialization."""
        return {
            "id": self.id,
            "keyword": self.keyword,
            "source": self.source,
            "title": self.title,
            "created_at": (
                self.created_at.isoformat()
                if isinstance(self.created_at, datetime)
                else self.created_at
            ),
            "body": self.body,
            "comments": self.comments,
        }

    def to_json(self) -> str:
        """Convert Post to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "Post":
        """Create Post from dictionary."""
        # Convert ISO format string back to datetime
        if isinstance(data["created_at"], str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> "Post":
        """Create Post from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def persist(self):
        """Persist the Post to S3"""
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=os.environ["S3_BUCKET_NAME"],
            Key=f"posts/{self.id}.json",
            Body=self.to_json(),
            ContentType="application/json",
        )
        self.logger.debug("Persisted key %s to S3", f"posts/{self.id}.json")

    # construct Post from s3
    @classmethod
    def from_s3(
        cls, post_id: str, logger: logging.Logger | None = None
    ) -> "Post":
        """Construct Post from S3"""
        s3 = boto3.client("s3")
        if logger is None:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
        logger.debug("Fetching key %s", f"posts/{post_id}.json")
        response = s3.get_object(
            Bucket=os.environ["S3_BUCKET_NAME"], Key=f"posts/{post_id}.json"
        )
        return cls.from_json(response["Body"].read().decode("utf-8"))
