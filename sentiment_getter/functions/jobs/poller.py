"""
Lambda function to poll for sentiment analysis job completion and process results.
"""

import json
import os
import logging
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from supabase import create_client, Client

# Import the provider factory
from providers.provider_factory import get_provider

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource("dynamodb")

# Constants
JOBS_TABLE_NAME = os.environ["JOBS_TABLE_NAME"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def lambda_handler(_, __):
    """
    Poll for sentiment analysis jobs that are in SUBMITTED or IN_PROGRESS state
    and process completed jobs.
    """
    # Get the appropriate provider
    provider = get_provider()

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
    pending_jobs = response.get("Items", []) + in_progress_response.get("Items", [])
    logger.info("Found %d pending jobs", len(pending_jobs))

    processed_jobs = 0
    for job in pending_jobs:
        job_id = job["job_id"]
        current_version = job["version"]

        # Check job status using the provider
        status, output_file_id = provider.check_job_status(job)

        # Update status in DynamoDB with optimistic locking
        if status != job["status"]:
            success, current_version = update_job_status(
                job_id, status, current_version
            )
            if not success:
                continue
            logger.info("Updated job status from %s to %s ", job["status"], status)

        # Process completed jobs
        if status == "COMPLETED":
            success, current_version = update_job_status(
                job_id, "STORING", current_version
            )
            if not success:
                continue

            # Process the completed job using the provider
            provider.process_completed_job(job, output_file_id, current_version)
            processed_jobs += 1

        elif status == "FAILED":
            logger.error("Job %s failed", job_id)
            success, current_version = update_job_status(
                job_id, "FAILED", current_version
            )
            if not success:
                continue

    return {
        "statusCode": 200,
        "body": json.dumps(
            f"Processed {processed_jobs} completed jobs out of {len(pending_jobs)} pending jobs"
        ),
    }


def update_job_status(job_id: str, status: str, version: int) -> tuple[bool, int]:
    """Update job status with optimistic locking"""
    try:
        # Calculate TTL for 30 days from now
        ttl = int((datetime.now() + timedelta(days=30)).timestamp())

        # If version is 0, it means this is the first update
        if version == 0:
            response = jobs_table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #status = :status, version = :version, #ttl = :ttl",
                ConditionExpression="attribute_not_exists(version) OR version = :current_version",
                ExpressionAttributeNames={"#status": "status", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":status": status,
                    ":version": 1,
                    ":current_version": 0,
                    ":ttl": ttl,
                },
                ReturnValues="ALL_NEW",
            )
        else:
            response = jobs_table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #status = :status, version = :version, #ttl = :ttl",
                ConditionExpression="version = :current_version",
                ExpressionAttributeNames={"#status": "status", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":status": status,
                    ":version": version + 1,
                    ":current_version": version,
                    ":ttl": ttl,
                },
                ReturnValues="ALL_NEW",
            )
        return True, response["Attributes"]["version"]
    except ClientError as e:
        logger.error("Error updating job %s: %s", job_id, str(e))
        return False, version
