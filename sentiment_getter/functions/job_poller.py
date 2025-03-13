"""
Lambda function to poll for Comprehend job completion and process results.
"""

import json
import os
import logging
from datetime import datetime
import tarfile
import io
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
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)
JOB_OUTPUT_PREFIX = "comprehend-jobs/output/"


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

        try:
            # Check job status
            job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
            status = job_details["SentimentDetectionJobProperties"]["JobStatus"]
            message = job_details["SentimentDetectionJobProperties"].get("Message", "")
            # print OutputDataConfig
            logger.info(
                "OutputDataConfig: %s",
                job_details["SentimentDetectionJobProperties"]["OutputDataConfig"],
            )

            # Update status in DynamoDB
            if status != job["status"]:
                jobs_table.update_item(
                    Key={"job_id": job_id},
                    UpdateExpression="SET #status = :status, #error_message = :message",
                    ExpressionAttributeNames={
                        "#status": "status",
                        "#message": "message",
                    },
                    ExpressionAttributeValues={
                        ":status": status,
                        ":message": message,
                    },
                )
                logger.info("Updated job %s status to %s", job_id, status)

            # Process completed jobs
            if status == "COMPLETED":
                process_completed_job(job)
                processed_jobs += 1
            elif status == "FAILED":
                logger.error(
                    "Job %s failed: %s",
                    job_id,
                    job_details["SentimentDetectionJobProperties"].get("Message"),
                )
                jobs_table.update_item(
                    Key={"job_id": job_id},
                    UpdateExpression="SET #status = :status, error_message = :error",
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={":status": "FAILED", ":error": message},
                )

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


def process_completed_job(job):
    """Process a completed Comprehend job"""
    job_id = job["job_id"]
    keyword = job.get("keyword", "unknown")
    source = job.get("source", "reddit")
    post_metadata = job.get("post_metadata", {})

    logger.info("Processing completed job %s", job_id)

    try:
        # Get job details to get the output location
        job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
        output_s3_uri = job_details["SentimentDetectionJobProperties"][
            "OutputDataConfig"
        ]["S3Uri"]

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
                        for i, line in enumerate(lines):
                            result = json.loads(line)

                            # Get post metadata if available
                            post_info = post_metadata.get(str(i), {})
                            post_id = post_info.get("id", f"job_{job_id}_{i}")
                            post_title = post_info.get("title", "")
                            created_at = post_info.get(
                                "created_at", datetime.now().isoformat()
                            )

                            # Parse created_at to datetime for partitioning
                            if isinstance(created_at, str):
                                created_time = datetime.fromisoformat(created_at)
                            else:
                                created_time = datetime.now()

                            # Create S3 path with partitioning
                            path = (
                                f"sentiment/keyword={keyword}/"
                                f"year={created_time.year}/month={created_time.month:02d}/"
                                f"day={created_time.day:02d}/"
                                f"source={source}/{source}_{post_id}.json"
                            )

                            # Store result data
                            result_data = {
                                "source": source,
                                "id": post_id,
                                "post_title": post_title,
                                "created_time": created_at,
                                "sentiment": result["Sentiment"],
                                "sentiment_score": result["SentimentScore"],
                                "job_id": job_id,
                            }

                            logger.info("Storing results in S3 at %s", path)
                            s3.put_object(
                                Bucket=BUCKET_NAME,
                                Key=path,
                                Body=json.dumps(result_data, indent=2),
                                ContentType="application/json",
                            )
                            processed += 1

        # Update job status to PROCESSED in DynamoDB
        jobs_table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=("SET #status = :status, processed_at = :time"),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "PROCESSED",
                ":time": datetime.now().isoformat(),
            },
        )

        logger.info("Successfully processed %d results for job %s", processed, job_id)

    except ClientError as e:
        logger.error("AWS error processing results for job %s: %s", job_id, str(e))
        # Update job status to ERROR in DynamoDB
        jobs_table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #status = :status, error_message = :error",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": "ERROR", ":error": str(e)},
        )
    except (json.JSONDecodeError, tarfile.TarError, io.UnsupportedOperation) as e:
        logger.error("Data processing error for job %s: %s", job_id, str(e))
        jobs_table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #status = :status, error_message = :error",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": "ERROR", ":error": str(e)},
        )
