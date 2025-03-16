"""
Abstract interface for sentiment analysis providers.
"""

from abc import ABC, abstractmethod
from model.job import Job
from model.post import Post
from model.sentiment import Sentiment


class SentimentProvider(ABC):
    """Base interface for sentiment analysis providers."""

    @abstractmethod
    def create_sentiment_job(self, posts: list[Post], job_name: str) -> Job:
        """
        Create a sentiment analysis job for a batch of posts.

        Args:
            posts: List of Post objects to analyze
            job_name: Name for the job
        """

    @abstractmethod
    def get_provider_name(self) -> str:
        """
        Get the name of the provider.

        Returns:
            String name of the provider
        """

    @abstractmethod
    def query_and_update_job(self, job: Job):
        """
        Check the status of a sentiment analysis job from provider's API
        and update the job object with the new status and provider data.
        """

    @abstractmethod
    def process_completed_job(self, job: Job) -> list[Sentiment]:
        """
        Returns the sentiments for a completed job (multiple posts).
        """
