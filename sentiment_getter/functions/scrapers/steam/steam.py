"""
Lambda function to scrape Steam reviews for sentiment analysis.
"""

import json
import logging

from functions.scrapers.steam.scraper import get_steam_reviews

SOURCE = "steam"  # Define source for this scraper

def lambda_handler(event, _):
    """
    Scrape Steam reviews and return them for sentiment analysis.
    """
    logger = logging.getLogger("steam scraper")
    logger.setLevel(logging.INFO)

    logger.info(
        "Starting Steam scraper with parameters: %s", json.dumps(event, indent=2)
    )

    # Get reviews from Steam
    posts = get_steam_reviews(
        keyword=event["keyword"],
        sort=event.get("sort", "top"),
        time_filter=event.get("time_filter", "day"),
        post_limit=event.get("post_limit", 8),
        logger=logger,
    )
    # Set execution id
    for post in posts:
        post.execution_id = event["ephermeral_execution_id"]

    logger.info("Found %d reviews matching criteria", len(posts))

    return {
        "posts": [post.to_dict() for post in posts],
        "source": SOURCE,
        "keyword": event["keyword"]
    }
