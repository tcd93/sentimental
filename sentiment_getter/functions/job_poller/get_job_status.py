"""
Lambda function to get the status of a sentiment analysis job
"""

import logging

from model.job import Job
from providers.provider_factory import get_provider


def lambda_handler(event, _):
    """
    Get the status of a sentiment analysis job.
    Event should be the job object.

    Returns:
        Job object with the updated status
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    job = Job.from_dict(event, logger)
    provider = get_provider(logger=logger, provider_name=job.provider)

    return provider.query_and_update_job(job).to_dict()
