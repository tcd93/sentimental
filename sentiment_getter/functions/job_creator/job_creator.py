"""
Lambda function to create sentiment analysis jobs from batches of posts.
"""

import json
import logging
from datetime import datetime

from functions.athena import get_posts
from sentiment_service_providers.service_provider_factory import get_service_provider


class NoPostsFoundError(Exception):
    """Custom exception raised when no posts are found for a job."""


def lambda_handler(event, _):
    """
    Create a sentiment analysis job for posts from multiple scrapers.

    Args:
        sample event: {
            "ErrorCount": 0,
            "ExecutionID": "d869fdf2-5b00-40a6-a39c-8148ca75e68b",
            "MaxCreatedAt": 1743225996000000,
            "MinCreatedAt": 1743138377000000,
            "SuccessCount": 825
        }

    Returns:
        None if no posts are found, otherwise a dict containing job information
    """
    # Configure logging
    logger = logging.getLogger("job creator")
    logger.setLevel(logging.INFO)
    logger.debug(
        "Creating sentiment analysis job with parameters: %s", json.dumps(event)
    )

    execution_id = event["ExecutionID"]

    posts = get_posts(execution_id)
    if len(posts) == 0:
        logger.error("No posts found for execution ID: %s", execution_id)
        raise NoPostsFoundError(f"No posts found for execution ID: {execution_id}")

    provider = get_service_provider(logger=logger)

    # validate that all posts have the same execution id
    execution_id = posts[0].execution_id
    if not all(post.execution_id == execution_id for post in posts):
        raise ValueError("All posts must have the same execution id")

    # remove posts that have duplicate ids
    posts = list({post.id: post for post in posts}.values())

    job_name = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    job = provider.create_sentiment_job(posts, job_name)

    return {
        "ExecutionID": execution_id,
        "MaxCreatedAt": event["MaxCreatedAt"],
        "MinCreatedAt": event["MinCreatedAt"],
        "Job": job.to_dict(),
    }
