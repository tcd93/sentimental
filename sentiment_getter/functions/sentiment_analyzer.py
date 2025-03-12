"""
Lambda function to analyze sentiment of posts and send results to storage queue.
"""

import json
import os
import logging
from datetime import datetime
import boto3
from model.post import Post

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")
comprehend = boto3.client("comprehend")
SENTIMENT_QUEUE_URL = os.environ["SENTIMENT_QUEUE_URL"]


def analyze_sentiment(posts: list[Post]) -> dict | None:
    """Analyze sentiment of text using AWS Comprehend."""
    if not posts:
        return {
            "ResultList": [],
            "ErrorList": [],
        }
    
    all_comments = []
    for post in posts:
        if post.get_comment_text() != "":
            all_comments.append(post.get_comment_text())
    
    # {
    #     "ErrorList": [ 
    #         { 
    #             "ErrorCode": "string",
    #             "ErrorMessage": "string",
    #             "Index": number
    #         }
    #     ],
    #     "ResultList": [ 
    #         { 
    #             "Index": number,
    #             "Sentiment": "string",
    #             "SentimentScore": { 
    #                 "Mixed": number,
    #                 "Negative": number,
    #                 "Neutral": number,
    #                 "Positive": number
    #             }
    #         }
    #     ]
    # }
    if len(all_comments) > 0:
        return comprehend.batch_detect_sentiment(
            TextList=all_comments,
            LanguageCode="en",
        )

    return {
        "ResultList": [],
        "ErrorList": [],
    }


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
    logger.info(f"Processing {len(event['Records'])} records")

    posts: list[Post] = []
    for record in event["Records"]:
        # Parse message
        data = json.loads(record["body"])
        logger.info(f"Processing post data: {json.dumps(data, indent=2)}")
        
        post_data = data["post"]
        post_data["created_at"] = datetime.fromisoformat(post_data["created_at"])
        post = Post(**post_data)
        posts.append(post)

    # Analyze sentiment
    sentiment = analyze_sentiment(posts)
    if len(sentiment["ResultList"]) == 0:
        logger.warning("Error analyzing sentiment for posts")
        return {
            "statusCode": 200,
            "body": "No sentiment results returned for posts",
        }
    if len(sentiment["ErrorList"]) > 0:
        logger.warning("Error analyzing sentiment for posts")
        return {
            "statusCode": 200,
            "body": f"Error analyzing sentiment for posts, {sentiment['ErrorList'][0]}",
        }
    
    for result in sentiment["ResultList"]:
        post: Post = posts[result["Index"]]
        # Send results to storage queue
        # TODO: all these serializing and deserializing is a pain
        message_data = {
            "post": {
                "id": post.id,
                "title": post.title,
                "created_at": post.created_at.isoformat(),
                "comments": post.comments,
            },
            "keyword": data["keyword"],
            "sentiment": result["Sentiment"],
            "sentiment_score": result["SentimentScore"],
            "source": data["source"],
        }
        
        logger.info(f"Sending message to queue")
        sqs.send_message(
            QueueUrl=SENTIMENT_QUEUE_URL,
            MessageBody=json.dumps(message_data),
        )
        processed += 1

    logger.info(f"Successfully processed {processed} posts")
    return {
        "statusCode": 200,
        "body": json.dumps(f"Processed sentiment for {processed} posts"),
    }
