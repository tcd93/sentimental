"""
Lambda function to run after Mapping job bucket.
"""

import logging

from functions.athena import get_posts, save_sentiments
from models.job import Job
from models.post import Post
from sentiment_service_providers.service_provider_factory import get_service_provider


def lambda_handler(event, _):
    """
    Update Post sentiments back to Iceberg table.

    Sample event:
    {
        "ExecutionID": "d869fdf2-5b00-40a6-a39c-8148ca75e68b",
        "MaxCreatedAt": 1743225996000000,
        "MinCreatedAt": 1743138377000000,
        "Job": {...}
    }
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    execution_id = event["ExecutionID"]
    max_created_at = int(event["MaxCreatedAt"]) / 1_000_000
    min_created_at = int(event["MinCreatedAt"]) / 1_000_000

    job = Job.from_dict(event["Job"])
    assert job.status == "COMPLETED", "Job is not completed"

    provider = get_service_provider(logger=logger, provider_name=job.provider)

    posts: list[Post] = get_posts(
        execution_id, max_created_at, min_created_at
    )
    logger.info("Retrieved %d posts for job %s", len(posts), job.job_id)
    sentiments = provider.process_completed_job(job, posts)
    logger.info("Completed calculation of sentiments for job %s", job.job_id)
    save_sentiments(sentiments, min_created_at, max_created_at)
    logger.info("Saved sentiments for job %s", job.job_id)
