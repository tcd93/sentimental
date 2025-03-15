"""
Lambda function to create sentiment analysis jobs from batches of posts.
"""

import json
import os
import logging
from datetime import datetime
import boto3
from model.post import Post
from providers.provider_factory import get_provider

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource("dynamodb")

# Constants
JOBS_TABLE_NAME = os.environ["JOBS_TABLE_NAME"]
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)


def lambda_handler(event, _):
    """
    Create a sentiment analysis job for posts from multiple scrapers.

    Args:
        event: Array of sources, each with a list of Post objects

    Returns:
        Dict containing job information
    """
    logger.info(
        "Creating sentiment analysis job with parameters: %s", json.dumps(event)
    )

    # event must be a list
    if not isinstance(event, list):
        raise ValueError("Invalid input format: event must be a list")
    # each element in event must be a list
    for source in event:
        if not isinstance(source, list):
            raise ValueError(
                "Invalid input format: each source in event must be a list"
            )

    # flatten and convert to list of serialized Post objects
    posts: list[Post] = [Post.from_json(post) for source in event for post in source]

    if not posts or len(posts) == 0:
        return {"statusCode": 404, "body": json.dumps("No posts to analyze")}

    job_name = f"job_{datetime.now().strftime("%Y%m%d_%H%M%S")}"
    provider = get_provider()
    job_result = provider.create_sentiment_job(posts, job_name)
    logger.info("Created sentiment job: %s", json.dumps(job_result))

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": (
                    f"Started sentiment analysis for {len(posts)} posts "
                    "using {provider.get_provider_name()}"
                ),
                "job_id": job_result["job_id"],
                "job_name": job_name,
                "provider": provider.get_provider_name(),
            }
        ),
    }
