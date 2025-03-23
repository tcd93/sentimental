"""
This module contains the Reddit scrapper.
It is used to scrape Reddit posts and comments for a given keyword.
"""

from datetime import datetime
import os
import logging
import praw
from openai import OpenAI
from praw.models import Subreddit, Submission
from model.post import Post


# Initialize Reddit client only when needed
def get_reddit_client():
    """Get a configured Reddit client using environment variables."""
    if not os.environ["REDDIT_CLIENT_ID"] or not os.environ["REDDIT_CLIENT_SECRET"]:
        raise ValueError("REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET is not set")
    return praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ.get("REDDIT_USER_AGENT", "sentiment-bot"),
    )


def get_openai_client():
    """Get a configured OpenAI client using environment variables."""
    if not openai_set():
        raise ValueError("OPENAI_API_KEY is not set")
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"])


def openai_set() -> bool:
    """Check if the OpenAI API key is set."""
    return os.environ.get("OPENAI_API_KEY") is not None


def get_subreddits_from_chatgpt(
    openai: OpenAI, keyword: str, logger: logging.Logger | None = None
) -> list[str]:
    """Get relevant subreddits for a keyword using ChatGPT.

    Args:
        keyword (str): The keyword to find relevant subreddits for
        openai_client (object, optional): OpenAI client for testing purposes

    Returns:
        list[str]: List of subreddit names (without the 'r/' prefix)
    """
    # Prompt for ChatGPT
    prompt = f"""
    I need exactly 3 most relevant subreddits to find posts about "{keyword}".
    Provide ONLY the subreddit names as a comma-separated list.
    Do not include further explanations.
    """

    # Call ChatGPT API
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant that "
                    "provides relevant subreddit recommendations."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        max_tokens=100,
        temperature=0.7,
    )

    # Extract subreddits from response
    subreddit_text = response.choices[0].message.content.strip()
    # Split by comma and clean up each subreddit name, remove "r/" prefix
    subreddits = [
        s.strip().lower().replace("r/", "") for s in subreddit_text.split(",")
    ]

    if logger:
        logger.info("ChatGPT suggested subreddits for '%s': %s", keyword, subreddits)
    return subreddits


def get_reddit_posts(**kwargs) -> list[Post]:
    """Get Reddit posts matching search criteria.

    Args:
        kwargs (dict[str, any]): Dictionary containing:
            subreddits (list[str]): List of subreddit names to search (without 'r/' prefix).
                                    If empty, ChatGPT will suggest relevant subreddits.
            keyword (str): Search term to find posts
            sort (str): How to sort results ('relevance', 'hot', 'top', 'new', 'comments')
            time_filter (str): Time window to search ('all', 'day', 'hour', 'month', 'week', 'year')
            post_limit (int): Maximum number of posts to retrieve
            top_comments_limit (int): Number of top comments to get per post

    Returns:
        list[Post]: List of Post objects containing matched posts and their top comments
    """
    subreddits = kwargs.get("subreddits", [])
    keyword = kwargs["keyword"]
    sort = kwargs.get("sort", "top")
    time_filter = kwargs.get("time_filter", "day")
    post_limit = kwargs.get("post_limit", 7)
    top_comments_limit = kwargs.get("top_comments_limit", 3)
    logger = kwargs.get("logger", None)

    # use chatgpt to suggest subreddits
    if not subreddits or len(subreddits) == 0:
        if not openai_set():
            subreddits = ["all"]
        else:
            subreddits = get_subreddits_from_chatgpt(
                openai=get_openai_client(), keyword=keyword
            )
    else:
        subreddits = ["all"]

    posts = []
    reddit = get_reddit_client()
    subreddit: Subreddit = reddit.subreddit("+".join(subreddits))
    for post in subreddit.search(
        query=keyword, sort=sort, time_filter=time_filter, limit=post_limit
    ):
        post: Submission = post
        post.comments.replace_more(limit=0)
        top_comments = map(
            lambda c: c.body[:360] + "..." if len(c.body) > 360 else c.body,
            post.comments.list()[:top_comments_limit],
        )

        if post.num_comments >= 2:
            posts.append(
                Post(
                    id=post.id,
                    keyword=keyword,
                    source="reddit",
                    title=post.title,
                    created_at=datetime.fromtimestamp(post.created_utc),
                    body=(
                        post.selftext[:720] + "..."
                        if len(post.selftext) > 720
                        else post.selftext
                    ),
                    comments=list(top_comments),
                    post_url=f"https://reddit.com{post.permalink}",
                    logger=logger,
                )
            )

    return posts
