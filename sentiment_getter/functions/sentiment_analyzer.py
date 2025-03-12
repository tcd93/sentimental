"""
Lambda function to analyze sentiment of posts and send results to storage queue.
"""

import json
import os
from datetime import datetime
import boto3
from model.post import Post

sqs = boto3.client("sqs")
comprehend = boto3.client("comprehend")
SENTIMENT_QUEUE_URL = os.environ["SENTIMENT_QUEUE_URL"]


def analyze_sentiment(posts: list[Post]):
    """Analyze sentiment of text using AWS Comprehend."""
    if not posts:
        return {
            "ResultList": [],
            "ErrorList": [],
        }

    response = comprehend.batch_detect_sentiment(
        TextList=[post.get_comment_text() for post in posts],
        LanguageCode="en",
    )
    return response


def lambda_handler(event, _):
    """
    Process posts from SQS and analyze sentiment.

    Args:
        event: SQS event containing posts
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

        # Analyze sentiment
        sentiment = analyze_sentiment([post])
        if not sentiment["ResultList"]:
            continue

        # Send results to storage queue
        sqs.send_message(
            QueueUrl=SENTIMENT_QUEUE_URL,
            MessageBody=json.dumps(
                {
                    "post": data["post"],
                    "keyword": data["keyword"],
                    "sentiment": sentiment["ResultList"][0],
                    "source": data["source"],
                }
            ),
        )
        processed += 1

    return {
        "statusCode": 200,
        "body": json.dumps(f"Processed sentiment for {processed} posts"),
    }
