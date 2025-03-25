"""
Module for scraping Steam reviews.
"""

import logging
from datetime import datetime
import requests
from models.post import Post

STEAM_API_URL = "https://store.steampowered.com/api/storesearch"
STEAM_REVIEWS_API_URL = "https://store.steampowered.com/appreviews/{app_id}"


def get_app_id(game_name):
    """
    Get Steam app ID for a game name.

    Args:
        game_name: Name of the game to search for

    Returns:
        app_id: Steam app ID or None if not found
    """
    params = {"term": game_name, "l": "english", "cc": "US"}
    response = requests.get(STEAM_API_URL, params=params, timeout=8)
    response.raise_for_status()
    data = response.json()

    if data.get("total", 0) > 0:
        return data["items"][0]["id"]

    return None


def get_steam_reviews(
    keyword,
    execution_id: str,
    time_filter="day",
    sort="top",
    post_limit=10,
    logger: logging.Logger | None = None,
) -> list[Post]:
    """
    Get Steam reviews for a game.

    Args:
        keyword: Name of the game
        time_filter: Time filter for reviews (day, week, month, year, all)
        sort: Sort method (created, updated, top)
        post_limit: Number of reviews to fetch

    Returns:
        list: List of Post objects containing reviews
    """
    app_id = get_app_id(keyword)
    if not app_id:
        if logger:
            logger.error("Could not find Steam app ID for game: %s", keyword)
        return []

    # Convert time filter to Steam's format
    time_ranges = {"day": 1, "week": 7, "month": 30, "year": 365, "all": 0}
    days = time_ranges.get(time_filter, 1)

    # Convert sort to Steam's format
    review_sort = {
        "top": "helpfulness",  # Most helpful reviews
        "new": "created",  # Most recent
        "hot": "helpfulness",  # Default to most helpful
    }.get(sort, "helpfulness")

    params = {
        "json": 1,
        "language": "english",
        "filter": "all",  # Include all review types
        "review_type": "all",  # Both positive and negative reviews
        "purchase_type": "all",
        "num_per_page": post_limit,
        "day_range": days,
        "review_score": 0,  # All review scores
        "cursor": "*",
        "sort": review_sort,
    }

    response = requests.get(
        STEAM_REVIEWS_API_URL.format(app_id=app_id), params=params, timeout=10
    )
    response.raise_for_status()
    data = response.json()

    if not data.get("success", 0):
        if logger:
            logger.error("Failed to get reviews for app %s", app_id)
        return []

    posts = []
    for review in data.get("reviews", []):
        # Convert Steam review to Post format
        created_at = datetime.fromtimestamp(review["timestamp_created"])

        post = Post(
            id=f"steam_{review['recommendationid']}",
            keyword=keyword,
            source="steam",
            title=f"Steam Review for {keyword}",
            body=(
                review["review"][:960] + "..."
                if len(review["review"]) > 960
                else review["review"]
            ),
            created_at=created_at,
            comments=[],  # Steam reviews don't have comments in the same way
            post_url=(
                "https://steamcommunity.com/profiles/"
                f"{review['author']['steamid']}/recommended/{app_id}/"
            ),
            execution_id=execution_id,
        )
        posts.append(post)

    return posts
