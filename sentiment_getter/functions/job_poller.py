"""
Lambda function to poll for Comprehend job completion and process results.
"""

import json
import os
import logging
from datetime import datetime, timedelta
import tarfile
import io
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

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
RESULTS_TABLE_NAME = os.environ["RESULTS_TABLE_NAME"]
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)
results_table = dynamodb.Table(RESULTS_TABLE_NAME)
JOB_OUTPUT_PREFIX = "comprehend-jobs/output/"


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
                ExpressionAttributeNames={
                    "#status": "status",
                    "#ttl": "ttl"
                },
                ExpressionAttributeValues={
                    ":status": status,
                    ":version": 1,
                    ":current_version": 0,
                    ":ttl": ttl
                },
                ReturnValues="ALL_NEW"
            )
        else:
            response = jobs_table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #status = :status, version = :version, #ttl = :ttl",
                ConditionExpression="version = :current_version",
                ExpressionAttributeNames={
                    "#status": "status",
                    "#ttl": "ttl"
                },
                ExpressionAttributeValues={
                    ":status": status,
                    ":version": version + 1,
                    ":current_version": version,
                    ":ttl": ttl
                },
                ReturnValues="ALL_NEW"
            )
        return True, response["Attributes"]["version"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailed":
            logger.warning("Optimistic lock failed for job %s - version mismatch", job_id)
        else:
            logger.error("Error updating job %s: %s", job_id, str(e))
        return False, version


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
        KeyConditionExpression=boto3.dynamodb.conditions.Key("status").eq("IN_PROGRESS"),
    )

    # Combine results from both queries
    pending_jobs = response.get("Items", []) + in_progress_response.get("Items", [])
    logger.info("Found %d pending jobs", len(pending_jobs))

    processed_jobs = 0
    for job in pending_jobs:
        job_id = job["job_id"]
        current_version = job.get("version", 0)

        try:
            # Check job status
            job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
            status = job_details["SentimentDetectionJobProperties"]["JobStatus"]

            # Update status in DynamoDB with optimistic locking
            if status != job["status"]:
                success, current_version = update_job_status(job_id, status, current_version)
                if not success:
                    continue
                logger.info("Updated job %s status to %s", job_id, status)

            # Process completed jobs
            if status == "COMPLETED":
                success, current_version = update_job_status(job_id, "STORING", current_version)
                if not success:
                    continue
                process_completed_job(job, current_version)
                processed_jobs += 1
            elif status == "FAILED":
                logger.error("Job %s failed: %s",
                             job_id,
                             job_details["SentimentDetectionJobProperties"].get("Message", ""))
                success, current_version = update_job_status(job_id, "FAILED", current_version)
                if not success:
                    continue

        except ClientError as e:
            logger.error("AWS error processing job %s: %s", job_id, str(e))
        except KeyError as e:
            logger.error("Missing key in job data for %s: %s", job_id, str(e))

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

    try:
        # Get job details to get the output location
        job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
        output_s3_uri = job_details["SentimentDetectionJobProperties"]["OutputDataConfig"]["S3Uri"]

        # Extract the key from the S3 URI (remove 's3://bucket-name/' prefix)
        output_key = output_s3_uri.replace(f"s3://{BUCKET_NAME}/", "")
        logger.info("Retrieving output file from: %s", output_key)

        # Get output file
        response = s3.get_object(Bucket=BUCKET_NAME, Key=output_key)
        tar_content = response["Body"].read()
        logger.info("Successfully retrieved output file")

        # Extract and process results
        processed = 0
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

                            # Get post metadata directly since we're processing one post per job
                            post_id = post_metadata["id"]
                            post_title = post_metadata.get("title", "")
                            created_at = post_metadata.get("created_at", datetime.now().isoformat())

                            # Store result data in DynamoDB with flattened metadata
                            result_data = {
                                "keyword": keyword,
                                "created_time": created_at,
                                "source": source,
                                "post_id": post_id,
                                "post_title": post_title,
                                "sentiment": result["Sentiment"],
                                "sentiment_score": {
                                    "mixed": Decimal(str(result["SentimentScore"]["Mixed"])),
                                    "positive": Decimal(str(result["SentimentScore"]["Positive"])),
                                    "neutral": Decimal(str(result["SentimentScore"]["Neutral"])),
                                    "negative": Decimal(str(result["SentimentScore"]["Negative"]))
                                },
                                "job_id": job_id,
                                "ttl": int((datetime.now() + timedelta(days=1095)).timestamp())  # 3-year TTL
                            }

                            logger.info("Storing results in DynamoDB for keyword %s", keyword)
                            results_table.put_item(Item=result_data)
                            processed += 1

        # Update job status to PROCESSED in DynamoDB with optimistic locking
        success, current_version = update_job_status(job_id, "PROCESSED", current_version)
        if not success:
            logger.error("Failed to update job %s status - version mismatch", job_id)
            return

        logger.info("Successfully processed %d results for job %s", processed, job_id)

    except ClientError as e:
        logger.error("AWS error processing results for job %s: %s", job_id, str(e))
        # Update job status to ERROR in DynamoDB with optimistic locking
        success, current_version = update_job_status(job_id, "ERROR", current_version)
        if not success:
            logger.error("Failed to update job %s status - version mismatch", job_id)
    except (json.JSONDecodeError, tarfile.TarError, io.UnsupportedOperation) as e:
        logger.error("Data processing error for job %s: %s", job_id, str(e))
        success, current_version = update_job_status(job_id, "ERROR", current_version)
        if not success:
            logger.error("Failed to update job %s status - version mismatch", job_id)
