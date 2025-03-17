"""
Lambda function to create sentiment analysis jobs from batches of posts.
"""

import json
import logging
from datetime import datetime
from model.post import Post
from providers.provider_factory import get_provider

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, _):
    """
    Create a sentiment analysis job for posts from multiple scrapers.

    Args:
        event: Array of posts

    Returns:
        Dict containing job information
    """
    logger.debug(
        "Creating sentiment analysis job with parameters: %s", json.dumps(event)
    )

    # event must be a list
    if not isinstance(event, list):
        raise ValueError("Invalid input format: event must be a list")

    # flatten and convert to list of serialized Post objects
    posts: list[Post] = [Post.from_json(post) for post in event]

    if not posts or len(posts) == 0:
        return {"statusCode": 404, "body": json.dumps("No posts to analyze")}

    job_name = f"job_{datetime.now().strftime("%Y%m%d_%H%M%S")}"
    provider = get_provider()
    job = provider.create_sentiment_job(posts, job_name)
    job.persist()

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": (
                    f"Started sentiment analysis for {len(posts)} posts "
                    f"using {provider.get_provider_name()}"
                ),
                "job_id": job.job_id,
                "job_name": job_name,
                "provider": provider.get_provider_name(),
            }
        ),
    }
