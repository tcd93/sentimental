"""
Lambda function to store sentiment analysis results in S3.
"""

import json
import os
import logging
import boto3
from model.post import Post

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


def lambda_handler(event, _):
    """
    Store sentiment results in S3.

    Args:
        event: SQS event containing sentiment results
        _: Lambda context

    Returns:
        Dict containing status code and message
    """
    logger.info("Starting storage for %d messages", len(event.get("Records", [])))
    processed = 0

    for record in event.get("Records", []):
        # Parse message
        data = json.loads(record["body"])
        post_data = data["post"]

        # Create Post object using from_dict
        post = Post.from_dict(post_data)

        # Create S3 path with partitioning
        created_time = post.created_at
        path = (
            f"sentiment/keyword={data['keyword']}/"
            f"year={created_time.year}/month={created_time.month:02d}/day={created_time.day:02d}/"
            f"source={data['source']}/{data['source']}_{post.id}.json"
        )

        # Store in S3
        result_data = {
            "source": data["source"],
            "id": post.id,
            "post_title": post.title,
            "created_time": post.created_at.isoformat(),
            "sentiment": data["sentiment"]["Sentiment"],
            "sentiment_score": data["sentiment"]["SentimentScore"],
        }

        logger.info("Storing results in S3 at %s", path)
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=path,
            Body=json.dumps(result_data, indent=2),
            ContentType="application/json",
        )
        processed += 1

    return {"statusCode": 200, "body": json.dumps(f"Stored {processed} results in S3")}
