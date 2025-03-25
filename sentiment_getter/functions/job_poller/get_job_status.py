"""
Lambda function to get the status of a sentiment analysis job
"""

import logging
from models.job import Job
from sentiment_service_providers.service_provider_factory import get_service_provider


def lambda_handler(event, _):
    """
    Get the status of a sentiment analysis job.
    Event should be the job object.

    Returns:
        Job object with the updated status
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    job = Job.from_dict(event)
    provider = get_service_provider(logger=logger, provider_name=job.provider)

    status, provider_data = provider.query(job)

    return Job(
        job_id=job.job_id,
        job_name=job.job_name,
        status=status,
        created_at=job.created_at,
        post_ids=job.post_ids,
        provider=job.provider,
        provider_data=provider_data,
        execution_id=job.execution_id,
    ).to_dict()
