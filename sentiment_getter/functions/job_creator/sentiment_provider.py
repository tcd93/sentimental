"""
Abstract interface for sentiment analysis providers.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
from model.post import Post


class SentimentProvider(ABC):
    """Base interface for sentiment analysis providers."""

    @abstractmethod
    def create_sentiment_job(self, posts: List[Post], job_name: str) -> Dict[str, Any]:
        """
        Create a sentiment analysis job for a batch of posts.

        Args:
            posts: List of Post objects to analyze
            job_name: Name for the job

        Returns:
            Dict containing job information (job_id, job_name, etc.)
        """

    @abstractmethod
    def get_provider_name(self) -> str:
        """
        Get the name of the provider.

        Returns:
            String name of the provider
        """
