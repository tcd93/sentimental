"""
Lambda function to poll for sentiment analysis job
"""

import os
import logging
import boto3

from model.job import Job
from model.post import Post
from providers.provider_factory import get_provider

def lambda_handler(_, __):
    """
    Poll for sentiment analysis jobs that are in SUBMITTED or IN_PROGRESS state
    """
    # Configure logging
    logger = logging.getLogger("poller")
    logger.setLevel(logging.INFO)
    # Initialize AWS clients
    dynamodb = boto3.resource("dynamodb")

    jobs_table = dynamodb.Table(os.environ["JOBS_TABLE_NAME"])

    # Query DynamoDB for pending jobs
    response = jobs_table.query(
        IndexName="status-index",
        KeyConditionExpression="#status = :submitted",
        ExpressionAttributeNames={
            "#status": "status"
        },
        ExpressionAttributeValues={
            ":submitted": "SUBMITTED"
        }
    )

    # Get jobs with IN_PROGRESS status in a separate query
    in_progress_response = jobs_table.query(
        IndexName="status-index",
        KeyConditionExpression="#status = :in_progress",
        ExpressionAttributeNames={
            "#status": "status"
        },
        ExpressionAttributeValues={
            ":in_progress": "IN_PROGRESS"
        }
    )

    # Combine results from both queries
    pending_items = response.get("Items", []) + in_progress_response.get("Items", [])
    logger.debug("Found %d pending jobs", len(pending_items))

    provider = get_provider(logger)
    jobs = [Job.from_dict(item, logger) for item in pending_items]

    count = 0
    for job in jobs:
        provider.query_and_update_job(job)

        if job.status == "COMPLETED":
            job.persist()
            posts = [
                Post.from_s3(post_id, provider.get_provider_name())
                for post_id in job.post_ids
            ]

            sentiments = provider.process_completed_job(job, posts)
            if sentiments:
                job.status = "DB_SYNCING"
                job.persist()

                for sentiment in sentiments:
                    count += sentiment.sync_supabase()

                job.status = "DB_SYNCED"
                job.persist()

    return {"message": f"Jobs processed successfully. {count} records upserted."}
