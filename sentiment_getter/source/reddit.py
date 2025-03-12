"""
This module contains the Reddit class, which is used to get reddit posts.
"""

from datetime import datetime
import os

import praw
from praw.models import Subreddit
from model.post import Post

# Reddit Setup
reddit = praw.Reddit(
    client_id=os.environ["REDDIT_CLIENT_ID"],
    client_secret=os.environ["REDDIT_CLIENT_SECRET"],
    user_agent=os.environ.get("REDDIT_USER_AGENT", "sentiment-bot"),
)


def get_reddit_posts(**kwargs) -> list[Post]:
    """Get Reddit posts matching search criteria.

    Args:
        kwargs (dict[str, any]): Dictionary containing:
            subreddits (list[str]): List of subreddit names to search
            keyword (str): Search term to find posts
            sort (str): How to sort results ('relevance', 'hot', 'top', 'new', 'comments')
            time_filter (str): Time window to search ('all', 'day', 'hour', 'month', 'week', 'year')
            post_limit (int): Maximum number of posts to retrieve
            n (int): Number of top comments to get per post

    Returns:
        list[Post]: List of Post objects containing matched posts and their top comments
    """
    subreddits = kwargs["subreddits"]
    keyword = kwargs["keyword"]
    sort = kwargs.get("sort", "hot")
    time_filter = kwargs.get("time_filter", "year")
    post_limit = kwargs.get("post_limit", 5)
    n = kwargs.get("n", 5)

    posts = []
    subreddit: Subreddit = reddit.subreddit("+".join(subreddits))
    for post in subreddit.search(
        query=keyword, sort=sort, time_filter=time_filter, limit=post_limit
    ):
        post.comments.replace_more(limit=0)
        top_comments = map(lambda c: c.body, post.comments.list()[:n])

        posts.append(
            Post(
                id=post.id,
                title=post.title,
                created_at=datetime.fromtimestamp(post.created_utc),
                comments=list(top_comments),
            )
        )

    return posts
