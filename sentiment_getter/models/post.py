"""
This module contains the Post class, which is used to represent a post on a social media platform.
"""

from dataclasses import dataclass
from datetime import datetime
import logging
import json
import os
import boto3


@dataclass(frozen=True)
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
    execution_id: str
    post_url: str = ""  # Optional URL to the original post

    def get_text(self) -> str:
        """
        Get the text of the post and comments as a single line for processing
        """
        # Handle None values in comments
        safe_comments = [
            comment.replace("\n", ".") if comment is not None else ""
            for comment in self.comments or []
        ]
        trimmed_comments = " - ".join(safe_comments)

        # Handle None values in title and body
        safe_title = self.title.replace("\n", ".") if self.title is not None else ""
        safe_body = self.body.replace("\n", ".") if self.body is not None else ""

        return (
            f"title: {safe_title}; "
            f"body: {safe_body}; "
            f"comments: {trimmed_comments}"
        )

    def to_dict(self) -> dict:
        """Convert Post to dictionary for serialization."""
        return {
            "id": self.id,
            "execution_id": self.execution_id,
            "keyword": self.keyword,
            "source": self.source,
            "title": self.title,
            # convert to microsecond for Iceberg
            "created_at": self.created_at.timestamp() * 1_000_000,
            "body": self.body,
            "comments": self.comments,
            "post_url": self.post_url,
        }

    def to_json(self) -> str:
        """Convert Post to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "Post":
        """Create Post from dictionary."""
        if isinstance(data["created_at"], float):
            # convert from microsecond to second for datetime
            data["created_at"] = datetime.fromtimestamp(data["created_at"] / 1_000_000)
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> "Post":
        """Create Post from JSON string."""
        return cls.from_dict(json.loads(json_str))

    # construct Post from s3
    @classmethod
    def from_s3(cls, key: str, logger: logging.Logger | None = None) -> "Post":
        """Construct Post from S3"""
        s3 = boto3.client("s3")
        if logger is None:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
        logger.debug("Fetching key %s", key)
        response = s3.get_object(Bucket=os.environ["S3_BUCKET_NAME"], Key=key)
        return cls.from_json(response["Body"].read().decode("utf-8"))
