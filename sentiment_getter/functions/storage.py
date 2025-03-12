"""
Lambda function to store sentiment analysis results in S3.
"""

import json
import os
from datetime import datetime
import boto3
from model.post import Post

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
    processed = 0

    for record in event["Records"]:
        # Parse message
        data = json.loads(record["body"])
        post_data = data["post"]
        post_data["created_at"] = datetime.fromisoformat(post_data["created_at"])
        post = Post(**post_data)

        # Create S3 path with partitioning
        created_time = post.created_at
        path = (
            f"sentiment/keyword={data['keyword']}/"
            f"year={created_time.year}/month={created_time.month:02d}/day={created_time.day:02d}/"
            f"source={data['source']}/{data['source']}_{post.id}.json"
        )

        # Store in S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=path,
            Body=json.dumps(
                {
                    "source": data["source"],
                    "id": post.id,
                    "post_title": post.title,
                    "created_time": post.created_at.isoformat(),
                    "sentiment": data["sentiment"]["Sentiment"],
                    "sentiment_score": data["sentiment"]["SentimentScore"],
                }
            ),
            ContentType="application/json",
        )
        processed += 1

    return {"statusCode": 200, "body": json.dumps(f"Stored {processed} results in S3")}
