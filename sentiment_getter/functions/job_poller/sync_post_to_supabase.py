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
    Sync posts to Supabase after Mapping job bucket.
    Note: Item batching is enabled so expect object with Item list.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    s3 = boto3.client("s3")

    items = event["Items"]
    assert isinstance(items, list), "Items is not a list"

    # batch input is configured in Map state and key name is hardcoded
    job = event["BatchInput"]["Job"]
    job = Job.from_dict(job, logger)
    assert job.status == "COMPLETED", "Job is not completed"
    job.persist()

    provider = get_provider(logger=logger, provider_name=job.provider)

    posts = []
    for item in items:
        # skip zero-byte file returned from ListObjectsV2
        if item["Size"] == 0:
            continue
        response = s3.get_object(Bucket=os.environ["S3_BUCKET_NAME"], Key=item["Key"])
        post = Post.from_json(response["Body"].read().decode("utf-8"))
        posts.append(post)

    sentiments = provider.process_completed_job(job, posts)
    if sentiments:
        job.status = "DB_SYNCING"
        job.persist()

        for sentiment in sentiments:
            sentiment.sync_supabase()

        job.status = "DB_SYNCED"
        job.persist()

    # return list of keys for deletion
    return [{"Key": f"posts/{post.id}.json"} for post in posts]
