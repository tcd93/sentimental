"""
Lambda function to scrape Reddit posts for sentiment analysis.
"""

import json
import logging

from functions.scrapers.reddit.scrapper import get_reddit_posts

SOURCE = "reddit"  # Define source for this scraper


def lambda_handler(event, _):
    """
    Scrape Reddit posts and return them for sentiment analysis.
    """
    logger = logging.getLogger("reddit scraper")
    logger.setLevel(logging.INFO)

    logger.debug(
        "Starting Reddit scraper with parameters: %s", json.dumps(event, indent=2)
    )

    # Get posts from Reddit
    posts = get_reddit_posts(
        subreddits=event.get("subreddits", []),
        keyword=event["keyword"],
        sort=event.get("sort", "top"),
        time_filter=event.get("time_filter", "day"),
        post_limit=event.get("post_limit", 7),
        top_comments_limit=event.get("top_comments_limit", 3),
        logger=logger,
    )

    logger.info("Found %d posts matching criteria", len(posts))
    return {
        "posts": [post.to_dict() for post in posts],
        "source": SOURCE,
        "keyword": event["keyword"]
    }
