"""
Lambda function to poll for sentiment analysis job
"""

import os
import logging
import boto3

from model.job import Job
from providers.provider_factory import get_provider

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(_, __):
    """
    Poll for sentiment analysis jobs that are in SUBMITTED or IN_PROGRESS state
    """

    # Initialize AWS clients
    dynamodb = boto3.resource("dynamodb")

    jobs_table = dynamodb.Table(os.environ["JOBS_TABLE_NAME"])

    # Query DynamoDB for pending jobs
    response = jobs_table.query(
        IndexName="status-index",
        KeyConditionExpression=boto3.dynamodb.conditions.Key("status").eq("SUBMITTED"),
    )

    # Get jobs with IN_PROGRESS status in a separate query
    in_progress_response = jobs_table.query(
        IndexName="status-index",
        KeyConditionExpression=boto3.dynamodb.conditions.Key("status").eq(
            "IN_PROGRESS"
        ),
    )

    # Combine results from both queries
    pending_items = response.get("Items", []) + in_progress_response.get("Items", [])
    logger.info("Found %d pending jobs", len(pending_items))

    provider = get_provider()
    jobs = [Job.from_dict(item) for item in pending_items]

    count = 0
    for job in jobs:
        provider.query_and_update_job(job)

        if job.status == "COMPLETED":
            if not job.sync_dynamodb():
                continue

            sentiments = provider.process_completed_job(job)
            if sentiments:
                job.status = "DB_SYNCING"
                if not job.sync_dynamodb():
                    continue

                for sentiment in sentiments:
                    count += sentiment.sync_supabase()

                job.status = "DB_SYNCED"
                if not job.sync_dynamodb():
                    continue

    return {
        "statusCode": 200,
        "body": f"Jobs processed successfully. {count} records upserted.",
    }
