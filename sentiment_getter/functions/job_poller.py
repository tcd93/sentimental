"""
Lambda function to poll for Comprehend job completion and process results.
"""

import json
import os
import logging
from datetime import datetime, timedelta
import tarfile
import io
import boto3
from botocore.exceptions import ClientError
from supabase import create_client, Client
from postgrest import APIError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
comprehend = boto3.client("comprehend")
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Constants
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
JOBS_TABLE_NAME = os.environ["JOBS_TABLE_NAME"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)
JOB_OUTPUT_PREFIX = "comprehend-jobs/output/"

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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


def store_results_in_supabase(results):
    """Store sentiment results in Supabase using upsert to handle duplicates"""
    if not results:
        return

    records = [
        {
            "keyword": result["keyword"],
            "created_time": result["created_time"],
            "source": result["source"],
            "post_id": result["post_id"],
            "post_url": result["post_url"],
            "sentiment": result["sentiment"],
            "sentiment_score_mixed": float(result["sentiment_scores"]["Mixed"]),
            "sentiment_score_positive": float(result["sentiment_scores"]["Positive"]),
            "sentiment_score_neutral": float(result["sentiment_scores"]["Neutral"]),
            "sentiment_score_negative": float(result["sentiment_scores"]["Negative"]),
            "job_id": result["job_id"]
        }
        for result in results
    ]

    try:
        # Upsert records using Supabase client
        data = (supabase.table("sentiment_results")
                .upsert(records, on_conflict="post_id,job_id")
                .execute())
        logger.info("Successfully upserted %d results in Supabase", len(records))
        return data
    except APIError as e:
        logger.error("Failed to store results in Supabase: %s", str(e))
        raise


def lambda_handler(_, __):
    """
    Poll for Comprehend jobs that are in SUBMITTED or IN_PROGRESS state
    and process completed jobs.
    """
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
        current_version = job.get("version", 0)
        # Check job status
        job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
        status = job_details["SentimentDetectionJobProperties"]["JobStatus"]

        # Update status in DynamoDB with optimistic locking
        if status != job["status"]:
            success, current_version = update_job_status(
                job_id, status, current_version
            )
            if not success:
                continue
            logger.info("Updated job %s status to %s", job_id, status)

        # Process completed jobs
        if status == "COMPLETED":
            success, current_version = update_job_status(
                job_id, "STORING", current_version
            )
            if not success:
                continue
            process_completed_job(job, current_version)
            processed_jobs += 1
        elif status == "FAILED":
            logger.error(
                "Job %s failed: %s",
                job_id,
                job_details["SentimentDetectionJobProperties"].get("Message", ""),
            )
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


def process_completed_job(job, current_version):
    """Process a completed Comprehend job"""
    job_id = job["job_id"]
    keyword = job["keyword"]
    source = job["source"]
    post_metadata = job["post_metadata"]

    logger.info("Processing completed job %s", job_id)
    # Get job details to get the output location
    job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
    output_s3_uri = job_details["SentimentDetectionJobProperties"]["OutputDataConfig"][
        "S3Uri"
    ]

    # Extract the key from the S3 URI (remove 's3://bucket-name/' prefix)
    output_key = output_s3_uri.replace(f"s3://{BUCKET_NAME}/", "")
    logger.info("Retrieving output file from: %s", output_key)

    # Get output file
    response = s3.get_object(Bucket=BUCKET_NAME, Key=output_key)
    tar_content = response["Body"].read()
    logger.info("Successfully retrieved output file")

    # Extract and process results
    results_to_store = []
    with tarfile.open(fileobj=io.BytesIO(tar_content), mode="r:gz") as tar:
        for member in tar.getmembers():
            if member.name == "output":
                f = tar.extractfile(member)
                if f:
                    content = f.read().decode("utf-8")
                    lines = content.strip().split("\n")

                    # Process each line (one result per line)
                    for line in lines:
                        result = json.loads(line)
                        logger.info("Raw sentiment result: %s", json.dumps(result))

                        # Prepare result data
                        result_data = {
                            "keyword": keyword,
                            "created_time": post_metadata.get("created_at"),
                            "source": source,
                            "post_id": post_metadata["id"],
                            "post_url": post_metadata.get("post_url", ""),
                            "sentiment": result["Sentiment"],
                            "sentiment_scores": result["SentimentScore"],
                            "job_id": job_id,
                        }
                        results_to_store.append(result_data)

    store_results_in_supabase(results_to_store)

    success, current_version = update_job_status(job_id, "PROCESSED", current_version)
    if not success:
        logger.error("Failed to update job %s status - version mismatch", job_id)
