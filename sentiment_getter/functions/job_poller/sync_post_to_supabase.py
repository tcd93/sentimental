"""
Lambda function to run after Mapping job bucket.
"""

import logging
import os

from models.job import Job
from models.post import Post
from sentiment_service_providers.service_provider_factory import get_service_provider
import boto3


def lambda_handler(event, _):
    """
    Sync posts to Supabase. Input is a list of posts s3 keys (batch of 5 items)
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    s3 = boto3.client("s3")

    keys = event["Items"]
    assert isinstance(keys, list), "Items is not a list"

    # batch input is configured in Map state and key name is hardcoded
    job = event["BatchInput"]["Job"]
    job = Job.from_dict(job)
    assert job.status == "COMPLETED", "Job is not completed"

    provider = get_service_provider(logger=logger, provider_name=job.provider)

    posts: list[Post] = []
    for key in keys:
        response = s3.get_object(Bucket=os.environ["S3_BUCKET_NAME"], Key=key)
        post = Post.from_json(response["Body"].read().decode("utf-8"))
        posts.append(post)

    sentiments = provider.process_completed_job(job, posts)
    if sentiments:
        for sentiment in sentiments:
            sentiment.sync_supabase()

    # return list of keys for deletion
    return [{"Key": key} for key in keys]
