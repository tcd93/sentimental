"""
Lambda function to analyze sentiment of posts and send results to storage queue.
"""

import json
import os
import logging
import boto3
from model.post import Post

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")
comprehend = boto3.client("comprehend")
SENTIMENT_QUEUE_URL = os.environ["SENTIMENT_QUEUE_URL"]


def analyze_sentiment(posts: list[Post]):
    """
    Analyze sentiment of text using AWS Comprehend.

    Args:
        posts: List of Post objects to analyze

    Returns:
        dict: Map of post IDs to sentiment results and a list of errors
    """
    if not posts:
        return {"results": {}, "errors": []}

    # Filter out posts with no comments
    valid_posts = []
    post_id_map = {}  # Maps index to post ID

    for post in posts:
        comment_text = post.get_comment_text()
        if comment_text.strip():  # Only include posts with non-empty comments
            post_id_map[len(valid_posts)] = post.id
            valid_posts.append(post)

    if not valid_posts:
        logger.info("No posts with comments to analyze")
        return {"results": {}, "errors": []}

    # Call AWS Comprehend
    response = comprehend.batch_detect_sentiment(
        TextList=[post.get_comment_text() for post in valid_posts],
        LanguageCode="en",
    )

    # Convert list results to a map of post ID -> sentiment
    results_map = {}
    for result in response.get("ResultList", []):
        index = result["Index"]
        if index in post_id_map:
            post_id = post_id_map[index]
            results_map[post_id] = result

    return {"results": results_map, "errors": response.get("ErrorList", [])}


def lambda_handler(event, _):
    """
    Process posts from SQS and analyze sentiment.

    Args:
        event: SQS event containing posts
        _: Lambda context

    Returns:
        Dict containing status code and message
    """
    records = event.get("Records", [])
    logger.info("Starting sentiment analysis for %d messages", len(records))

    if not records:
        return {
            "statusCode": 200,
            "body": json.dumps("No records to process"),
        }

    # Collect all posts from records
    posts = []
    post_data_map = {}  # Map to store original data for each post

    for record in records:
        # Parse message
        data = json.loads(record["body"])
        post = Post.from_dict(data["post"])

        posts.append(post)
        # Store original data for later use
        post_data_map[post.id] = {
            "post": post,
            "keyword": data["keyword"],
            "source": data["source"],
        }

    # Batch analyze sentiment for all posts
    sentiment_results = analyze_sentiment(posts)
    processed = 0

    # Process results and send to storage queue
    for post_id, result in sentiment_results.get("results", {}).items():
        if post_id not in post_data_map:
            logger.error("Invalid post ID %s", post_id)
            continue

        post = post_data_map[post_id]["post"]
        original_data = post_data_map[post_id]

        # Send results to storage queue
        result_data = {
            "post": post.to_dict(),
            "keyword": original_data["keyword"],
            "sentiment": result,
            "source": original_data["source"],
        }

        logger.info("Sending sentiment results for post %s to storage queue", post.id)
        sqs.send_message(
            QueueUrl=SENTIMENT_QUEUE_URL, MessageBody=json.dumps(result_data)
        )
        processed += 1

    # Log any errors
    for error in sentiment_results.get("errors", []):
        logger.error("Error analyzing sentiment: %s", json.dumps(error))

    return {
        "statusCode": 200,
        "body": json.dumps(f"Processed sentiment for {processed} posts"),
    }
