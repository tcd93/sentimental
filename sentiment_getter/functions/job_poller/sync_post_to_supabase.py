"""
Lambda function to run after Mapping job bucket.
"""

import logging
import os

from model.job import Job
from model.post import Post
from providers.provider_factory import get_provider
import boto3


def lambda_handler(event, _):
    """
    Sync posts to Supabase. Input is a list of post keys.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    s3 = boto3.client("s3")

    keys = event["Items"]
    assert isinstance(keys, list), "Items is not a list"

    # batch input is configured in Map state and key name is hardcoded
    job = event["BatchInput"]["Job"]
    job = Job.from_dict(job, logger)
    assert job.status == "COMPLETED", "Job is not completed"

    provider = get_provider(logger=logger, provider_name=job.provider)

    posts: list[Post] = []
    for key in keys:
        response = s3.get_object(Bucket=os.environ["S3_BUCKET_NAME"], Key=key)
        post = Post.from_json(response["Body"].read().decode("utf-8"))
        posts.append(post)

    sentiments = provider.process_completed_job(job, posts)
    if sentiments:
        job.status = "DB_SYNCING"

        for sentiment in sentiments:
            sentiment.sync_supabase()

        job.status = "DB_SYNCED"

    # return list of keys for deletion
    return [{"Key": key} for key in keys]
