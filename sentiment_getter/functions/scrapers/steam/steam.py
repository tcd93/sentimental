"""
Lambda function to scrape Steam reviews for sentiment analysis.
"""

import json
import logging

from functions.scrapers.store_post_s3 import store_post_s3
from functions.scrapers.steam.scraper import get_steam_reviews
from providers.provider_factory import get_provider

SOURCE = "steam"  # Define source for this scraper

def lambda_handler(event, _):
    """
    Scrape Steam reviews and return them for sentiment analysis.

    Args:
        event: Dict containing search parameters
        _: Lambda context

    Returns:
        Dict containing posts and metadata
    """
    logger = logging.getLogger("steam scraper")
    logger.setLevel(logging.INFO)

    logger.info(
        "Starting Steam scraper with parameters: %s", json.dumps(event, indent=2)
    )

    # Get reviews from Steam
    posts = get_steam_reviews(
        keyword=event["keyword"],
        sort=event.get("sort", "hot"),
        time_filter=event.get("time_filter", "day"),
        post_limit=event.get("post_limit", 10),
        logger=logger,
    )

    logger.info("Found %d reviews matching criteria", len(posts))

    return store_post_s3(posts, get_provider(logger).get_provider_name())
