"""
This module contains the Sentiment class, which is used to represent the sentiment of a post.
"""

from dataclasses import dataclass
from models.post import Post


@dataclass(frozen=True)
class Sentiment:
    """Sentiment for a post."""

    post: Post
    sentiment: str
    mixed: float
    positive: float
    negative: float
    neutral: float
