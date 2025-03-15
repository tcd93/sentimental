"""
Lambda function to poll for sentiment analysis job completion and process results.
"""

import json
import os
import logging
from datetime import datetime, timedelta
import tarfile
import io
import boto3
import openai
from botocore.exceptions import ClientError
from supabase import create_client, Client
from model.post import Post

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
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
jobs_table = dynamodb.Table(JOBS_TABLE_NAME)
JOB_OUTPUT_PREFIX = "comprehend-jobs/output/"

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Configure OpenAI client
openai.api_key = OPENAI_API_KEY


def lambda_handler(_, __):
    """
    Poll for sentiment analysis jobs that are in SUBMITTED or IN_PROGRESS state
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
        current_version = job["version"]
        provider = job["provider"]
        if provider == "chatgpt" and not OPENAI_API_KEY:
            logger.error("ChatGPT provider selected but no API key provided")
            raise ValueError("ChatGPT provider selected but no API key provided")

        # Check job status based on provider
        if provider == "chatgpt":
            status, output_file_id = check_chatgpt_job_status(job)
        else:  # Default to Comprehend
            status = check_comprehend_job_status(job_id)

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

            if provider == "chatgpt":
                process_completed_chatgpt_job(job, output_file_id, current_version)
            else:
                process_completed_comprehend_job(job, current_version)

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


def check_comprehend_job_status(job_id):
    """Check the status of a Comprehend job"""
    job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
    return job_details["SentimentDetectionJobProperties"]["JobStatus"]


def check_chatgpt_job_status(job):
    """Check the status of a ChatGPT batch job"""
    batch_id = job["openai_batch_id"]
    job_id = job["job_id"]

    if not batch_id:
        logger.error("No OpenAI batch ID found for job %s", job_id)
        return "FAILED"

    batch_response = openai.batches.retrieve(batch_id)

    # Map OpenAI status to Comprehend status (because Comprehend is developed first)
    openai_status = batch_response.status
    if openai_status == "completed":
        return "COMPLETED", batch_response.output_file_id
    if openai_status in ["failed", "cancelled", "expired"]:
        logger.error("OpenAI batch job %s: %s", openai_status, batch_response)
        return "FAILED"
    if openai_status == "in_progress":
        return "IN_PROGRESS"


def process_completed_comprehend_job(job, current_version):
    """Process a completed Comprehend job"""
    job_id = job["job_id"]
    posts = job["posts"]

    logger.info("Processing completed Comprehend job %s", job_id)
    job_details = comprehend.describe_sentiment_detection_job(JobId=job_id)
    output_s3_uri = job_details["SentimentDetectionJobProperties"]["OutputDataConfig"][
        "S3Uri"
    ]

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
                    for line_number, line in enumerate(lines):
                        result = json.loads(line)

                        # Find the post metadata for the result, which is from the line number
                        post = Post.from_json(posts[line_number])

                        # Prepare result data
                        result_data = {
                            "keyword": post.keyword,
                            "created_time": post.created_at.isoformat(),
                            "source": post.source,
                            "post_id": post.id,
                            "post_url": post.post_url,
                            "sentiment": result["Sentiment"],
                            "sentiment_scores": result["SentimentScore"],
                            "job_id": job_id,
                        }
                        results_to_store.append(result_data)

    store_results_in_supabase(results_to_store)

    success, current_version = update_job_status(job_id, "PROCESSED", current_version)
    if not success:
        logger.error("Failed to update job %s status - version mismatch", job_id)


def process_completed_chatgpt_job(job, output_file_id, current_version):
    """Process a completed ChatGPT batch job"""
    job_id = job["job_id"]
    posts = [Post.from_json(post) for post in job["posts"]]

    if not output_file_id:
        logger.error("No output file ID found for job %s", job_id)
        update_job_status(job_id, "FAILED", current_version)
        raise ValueError("No output file ID found")


    logger.info("Processing completed ChatGPT job %s", job_id)

    content = openai.files.content(output_file_id)
    content_str = content.read().decode("utf-8")

    results_to_store = []
    lines = content_str.strip().split("\n")

    for line in lines:
        logger.info("Processing line: %s", line)
        openai_result = json.loads(line)
        if openai_result["response"]["status_code"] != 200:
            logger.warning("Unexpected response: %s", openai_result["response"])
            continue

        post_id = openai_result["custom_id"]
        if post_id not in [post.id for post in posts]:
            logger.warning("Post ID %s not found in job posts", post_id)
            continue

        # Find the corresponding post
        post = next(post for post in posts if post.id == post_id)

        # Extract sentiment data
        content = openai_result["response"]["body"]["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        sentiment = parsed["sentiment"]
        scores = parsed["scores"]

        # Prepare result data
        result_data = {
            "keyword": post.keyword,
            "created_time": post.created_at.isoformat(),
            "source": post.source,
            "post_id": post.id,
            "post_url": post.post_url,
            "sentiment": sentiment,
            "sentiment_scores": scores,
            "job_id": job_id,
        }
        results_to_store.append(result_data)

    store_results_in_supabase(results_to_store)

    success, current_version = update_job_status(
        job_id, "PROCESSED", current_version
    )
    if not success:
        logger.error(
            "Failed to update job %s status to PROCESSED - version mismatch", job_id
        )


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
            "job_id": result["job_id"],
        }
        for result in results
    ]

    data = (
        supabase.table("sentiment_results")
        .upsert(records, on_conflict="post_id,job_id")
        .execute()
    )
    if data.count is not None:
        logger.info("Successfully upserted %d records in Supabase", data.count)
    return data
