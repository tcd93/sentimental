"""
Lambda function to scrape Reddit posts for sentiment analysis.
"""

import json
import logging

from functions.scrapers.store_post_s3 import store_post_s3
from functions.scrapers.reddit.scrapper import get_reddit_posts
from providers.provider_factory import get_provider

SOURCE = "reddit"  # Define source for this scraper


def lambda_handler(event, _):
    """
    Scrape Reddit posts and return them for sentiment analysis.

    Args:
        event: Dict containing search parameters
        _: Lambda context

    Returns:
        Dict containing posts and metadata
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
        sort=event.get("sort", "hot"),
        time_filter=event.get("time_filter", "day"),
        post_limit=event.get("post_limit", 10),
        top_comments_limit=event.get("top_comments_limit", 10),
        logger=logger,
    )

    logger.info("Found %d posts matching criteria", len(posts))

    return store_post_s3(posts, get_provider(logger).get_provider_name())
