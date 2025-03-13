"""
Lambda function to scrape Reddit posts and send them to SQS for processing.
"""

from datetime import datetime
import json
import os
import logging
import boto3
import praw
from praw.models import Subreddit
from model.post import Post

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Reddit Setup
reddit = praw.Reddit(
    client_id=os.environ["REDDIT_CLIENT_ID"],
    client_secret=os.environ["REDDIT_CLIENT_SECRET"],
    user_agent=os.environ.get("REDDIT_USER_AGENT", "sentiment-bot"),
)

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
        subreddits=event.get("subreddits", ["all"]),
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


def get_reddit_posts(**kwargs) -> list[Post]:
    """Get Reddit posts matching search criteria.

    Args:
        kwargs (dict[str, any]): Dictionary containing:
            subreddits (list[str]): List of subreddit names to search
            keyword (str): Search term to find posts
            sort (str): How to sort results ('relevance', 'hot', 'top', 'new', 'comments')
            time_filter (str): Time window to search ('all', 'day', 'hour', 'month', 'week', 'year')
            post_limit (int): Maximum number of posts to retrieve
            top_comments_limit (int): Number of top comments to get per post

    Returns:
        list[Post]: List of Post objects containing matched posts and their top comments
    """
    subreddits = kwargs["subreddits"]
    keyword = kwargs["keyword"]
    sort = kwargs["sort"]
    time_filter = kwargs["time_filter"]
    post_limit = kwargs["post_limit"]
    top_comments_limit = kwargs["top_comments_limit"]

    posts = []
    subreddit: Subreddit = reddit.subreddit("+".join(subreddits))
    for post in subreddit.search(
        query=keyword, sort=sort, time_filter=time_filter, limit=post_limit
    ):
        post.comments.replace_more(limit=0)
        top_comments = map(lambda c: c.body, post.comments.list()[:top_comments_limit])

        posts.append(
            Post(
                id=post.id,
                title=post.title,
                created_at=datetime.fromtimestamp(post.created_utc),
                comments=list(top_comments),
            )
        )

    return posts
