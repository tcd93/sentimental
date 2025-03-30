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

    def to_dict(self) -> dict:
        """Convert the sentiment to a table row."""
        return {
            "keyword": self.post.keyword,
            "created_at": self.post.created_at,
            "execution_id": self.post.execution_id,
            "post_id": self.post.id,
            "post_url": self.post.post_url,
            "sentiment": self.sentiment,
            "sentiment_score_mixed": self.mixed,
            "sentiment_score_positive": self.positive,
            "sentiment_score_neutral": self.neutral,
            "sentiment_score_negative": self.negative,
        }

