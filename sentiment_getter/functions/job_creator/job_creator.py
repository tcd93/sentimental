"""
Lambda function to create sentiment analysis jobs from batches of posts.
"""

import json
import logging
from datetime import datetime
import os

import pandas as pd
from models.post import Post
from sentiment_service_providers.service_provider_factory import get_service_provider
import awswrangler as wr


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
    # Data is stored in Iceberg tables with microsecond precision
    # But when querying through Athena, the from_unixtime function expects seconds
    max_created_at = int(event["MaxCreatedAt"]) / 1_000_000
    min_created_at = int(event["MinCreatedAt"]) / 1_000_000

    # get posts from Athena
    posts = get_posts_from_athena(execution_id, max_created_at, min_created_at)
    if len(posts) == 0:
        return None

    provider = get_service_provider(logger=logger)

    # validate that all posts have the same execution id
    execution_id = posts[0].execution_id
    if not all(post.execution_id == execution_id for post in posts):
        raise ValueError("All posts must have the same execution id")

    job_name = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    job = provider.create_sentiment_job(posts, job_name, execution_id)

    return job.to_dict()


def get_posts_from_athena(
    execution_id: str, max_created_at: int, min_created_at: int
) -> list[Post]:
    """
    Get posts from Athena
    """
    table = "post"
    database = "sentimental"
    bucket = os.environ["S3_BUCKET_NAME"]
    query = f"""
        SELECT * FROM {database}.{table}
        WHERE execution_id = '{execution_id}'
            AND created_at >= from_unixtime({min_created_at})
            AND created_at <= from_unixtime({max_created_at})
    """
    # Execute Athena query and get results as pandas DataFrame
    df: pd.DataFrame = wr.athena.read_sql_query(
        sql=query,
        database=database,
        s3_output=f"s3://{bucket}/athena-results/",
        ctas_approach=False,  # must set to False to handle timestamp of Iceberg table
    )

    if df.empty:
        return []

    # Convert DataFrame rows to Post objects
    posts = []
    for row in df.to_dict(orient="records"):
        post = Post.from_dict(row)
        posts.append(post)

    return posts
