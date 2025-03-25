"""
Abstract interface for sentiment analysis providers.
"""

from abc import ABC, abstractmethod
import logging
from model.job import Job
from model.post import Post
from model.sentiment import Sentiment

class SentimentServiceProvider(ABC):
    """Base interface for sentiment analysis providers."""

    logger: logging.Logger | None = None

    def __init__(self, logger: logging.Logger | None = None):
        if logger is None:
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger

    @abstractmethod
    def create_sentiment_job(self, posts: list[Post], job_name: str, execution_id: str) -> Job:
        """
        Create a sentiment analysis job for a batch of posts.

        Args:
            posts: List of Post objects to analyze
            job_name: Name for the job
            execution_id: Step Functions execution ID for the job
        """

    @abstractmethod
    def get_provider_name(self) -> str:
        """
        Get the name of the provider.

        Returns:
            String name of the provider
        """

    @abstractmethod
    def query_and_update_job(self, job: Job) -> Job:
        """
        Check the status of a sentiment analysis job from provider's API
        and update the job object with the new status and provider data.
        """

    @abstractmethod
    def process_completed_job(self, job: Job, posts: list[Post]) -> list[Sentiment]:
        """
        Returns the sentiments for a completed job.
        """
