"""
Lambda function to create sentiment analysis jobs from batches of posts.
"""

import json
import logging
from datetime import datetime
from model.post import Post
from providers.provider_factory import get_provider


def lambda_handler(event, _):
    """
    Create a sentiment analysis job for posts from multiple scrapers.

    Args:
        event: Array of post ids

    Returns:
        Dict containing job information
    """
    # Configure logging
    logger = logging.getLogger("job creator")
    logger.setLevel(logging.INFO)
    logger.debug(
        "Creating sentiment analysis job with parameters: %s", json.dumps(event)
    )

    # event must be a list
    if not isinstance(event, list):
        raise ValueError("Invalid input format: event must be a list")

    provider = get_provider(logger=logger)

    start_time = datetime.now()
    logger.info("Start constructing posts")
    posts: list[Post] = [Post.from_s3(post_id, logger) for post_id in event]
    logger.info(
        "Posts constructed in %s seconds", (datetime.now() - start_time).total_seconds()
    )

    if not posts or len(posts) == 0:
        raise ValueError("No posts to analyze")

    # there might be posts with the same id, remove dupplicates by post id
    posts = list({post.id: post for post in posts}.values())

    job_name = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    job = provider.create_sentiment_job(posts, job_name)
    job.persist()

    return job.to_dict()
