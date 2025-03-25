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
        event: {
            "post_ids": [Post],
            "execution_id": execution id of the step function
        }

    Returns:
        Dict containing job information
    """
    # Configure logging
    logger = logging.getLogger("job creator")
    logger.setLevel(logging.INFO)
    logger.debug(
        "Creating sentiment analysis job with parameters: %s", json.dumps(event)
    )

    post_ids = event["post_ids"]
    execution_id = event["execution_id"]

    # event must be a list
    if not isinstance(post_ids, list):
        raise ValueError("Invalid input format: post_ids must be a list")

    provider = get_provider(logger=logger)

    start_time = datetime.now()
    logger.info("Start constructing posts")
    posts: list[Post] = [Post.from_s3(f"posts/{execution_id}/{id}.json", logger) for id in post_ids]
    logger.info(
        "Posts constructed in %s seconds", (datetime.now() - start_time).total_seconds()
    )

    if not posts or len(posts) == 0:
        raise ValueError("No posts to analyze")

    # validate that all posts have the same execution id
    execution_id = posts[0].execution_id
    if not all(post.execution_id == execution_id for post in posts):
        raise ValueError("All posts must have the same execution id")

    # there might be posts with the same id, remove dupplicates by post id
    posts = list({post.id: post for post in posts}.values())


    job_name = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    job = provider.create_sentiment_job(posts, job_name, execution_id)

    return job.to_dict()
