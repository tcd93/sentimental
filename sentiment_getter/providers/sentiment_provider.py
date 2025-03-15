"""
Abstract interface for sentiment analysis providers.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Tuple, Optional
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

    @abstractmethod
    def check_job_status(self, job: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        """
        Check the status of a sentiment analysis job.

        Args:
            job: Job metadata from DynamoDB

        Returns:
            Tuple of (status, output_file_id) where output_file_id may be None
        """

    @abstractmethod
    def process_completed_job(
        self, job: Dict[str, Any], output_file_id: str, current_version: int
    ) -> None:
        """
        Process a completed sentiment analysis job.

        Args:
            job: Job metadata from DynamoDB
            output_file_id: ID of the output file (if applicable)
            current_version: Current version of the job record for optimistic locking
        """
