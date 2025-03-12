"""
This lambda function is used to get the sentiment of a keyword from social media platforms
such as Reddit, Twitter, etc.
It will calculate the sentiment of the keyword and store the results in S3.

Parameters:
    keyword: str - The keyword to search for
    subreddits: list[str] - The subreddits to search for the keyword
    time_filter: str - The time filter to use for the search
    post_limit: int - The number of posts to return
"""

import json
from datetime import datetime
import os
import boto3
import praw

from model.post import Post
from source.reddit import get_reddit_posts

# AWS Clients
s3 = boto3.client("s3")
comprehend = boto3.client("comprehend")
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "your-bucket-name")

# Reddit Setup
reddit = praw.Reddit(
    client_id=os.environ["REDDIT_CLIENT_ID"],
    client_secret=os.environ["REDDIT_CLIENT_SECRET"],
    user_agent=os.environ.get("REDDIT_USER_AGENT", "sentiment-bot"),
)


def lambda_handler(event, _):
    """
    Main Lambda handler function
    Parameters:
        event: Dict containing the Lambda function event data
        context: Lambda runtime context
    Returns:
        Dict containing status message
    """

    # get keyword from event
    keyword = event["keyword"]
    subreddits = event.get("subreddits", ["leagueoflegends"])
    # "hot", "new", "top", "relevance"
    sort = event.get("sort", "hot")
    # "month", "week", "day", "hour", "all"
    time_filter = event.get("time_filter", "year")
    post_limit = event.get("post_limit", 3)
    if post_limit > 100:
        raise ValueError("Post limit must be less than 100")

    reddit_posts = get_reddit_posts(
        subreddits=subreddits,
        keyword=keyword,
        sort=sort,
        time_filter=time_filter,
        post_limit=post_limit,
        n=5,
    )

    # {
    #     'ResultList': [
    #         {
    #             'Index': 123,
    #             'Sentiment': 'POSITIVE'|'NEGATIVE'|'NEUTRAL'|'MIXED',
    #             'SentimentScore': {
    #                 'Positive': ...,
    #                 'Negative': ...,
    #                 'Neutral': ...,
    #                 'Mixed': ...
    #             }
    #         },
    #     ],
    #     'ErrorList': [
    #         {
    #             'Index': 123,
    #             'ErrorCode': 'string',
    #             'ErrorMessage': 'string'
    #         },
    #     ]
    # }
    sentiment_result = analyze_sentiment(reddit_posts)
    for result in sentiment_result["ResultList"]:
        post: Post = reddit_posts[result["Index"]]
        created_time: datetime = post.created_at
        path = (
            f"sentiment/keyword={keyword}/"
            + f"year={created_time.year}/month={created_time.month:02d}/day={created_time.day:02d}/"
            + f"source=reddit/reddit_{post.id}.json"
        )
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=path,
            Body=json.dumps(
                {
                    "source": "reddit",
                    "id": post.id,
                    "post_title": post.title,
                    "created_time": post.created_at.isoformat(),
                    "sentiment": result["Sentiment"],
                    "sentiment_score": result["SentimentScore"],
                }
            ),
        )

    return {
        "statusCode": 200,
        "body": json.dumps(f"Processed {len(sentiment_result['ResultList'])} posts"),
    }


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/comprehend.html
def analyze_sentiment(posts: list[Post]):
    """Analyze sentiment of text using AWS Comprehend."""
    if not posts or len(posts) == 0:
        return {
            "ResultList": [],
            "ErrorList": [],
        }

    response = comprehend.batch_detect_sentiment(
        TextList=[post.get_comment_text() for post in posts],
        LanguageCode="en",
    )
    return response
