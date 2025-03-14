"""
Lambda function to scrape Reddit posts and send them to SQS for processing.
"""
import json
import os
import logging
import boto3

from functions.reddit.scrapper import get_reddit_posts

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")
POSTS_QUEUE_URL = os.environ["POSTS_QUEUE_URL"]
SOURCE = "reddit"  # Define source for this scraper


def lambda_handler(event, _):
    """
    Scrape Reddit posts and send to SQS.

    Args:
        event: Dict containing search parameters
        _: Lambda context

    Returns:
        Dict containing status code and message
    """
    logger.info(
        "Starting Reddit scraper with parameters: %s",
        json.dumps(event, indent=2)
    )

    # Get posts from Reddit
    posts = get_reddit_posts(
        subreddits=event.get("subreddits", []),
        keyword=event["keyword"],
        sort=event.get("sort", "hot"),
        time_filter=event.get("time_filter", "day"),
        post_limit=event.get("post_limit", 10),
        top_comments_limit=event.get("top_comments_limit", 10),
    )

    logger.info("Found %d posts matching criteria", len(posts))

    # Send each post to SQS
    for post in posts:
        message_data = {
            "post": post.to_dict(),
            "keyword": event["keyword"],
            "source": SOURCE,
        }
        logger.info("Sending post %s to queue", post.id)

        sqs.send_message(
            QueueUrl=POSTS_QUEUE_URL,
            MessageBody=json.dumps(message_data),
        )

    return {
        "statusCode": 200,
        "body": json.dumps(f"Sent {len(posts)} posts to processing queue"),
    }


