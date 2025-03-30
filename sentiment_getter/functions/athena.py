"""Common function"""

import os

from models.post import Post
from models.sentiment import Sentiment
import awswrangler as wr
import pandas as pd


def get_posts(
    execution_id: str, max_created_at: int, min_created_at: int
) -> list[Post]:
    """
    Get posts from Athena
    """
    table = "post"
    database = "sentimental"
    bucket = os.environ["S3_BUCKET_NAME"]
    query = f"""
        SELECT id, execution_id, created_at, keyword, source, title, body, comments, post_url
        FROM {database}.{table}
        WHERE execution_id = '{execution_id}'
            AND created_at BETWEEN from_unixtime({min_created_at})
            AND from_unixtime({max_created_at})
    """
    # Execute Athena query and get results as pandas DataFrame
    df: pd.DataFrame = wr.athena.read_sql_query(
        sql=query,
        database=database,
        s3_output=f"s3://{bucket}/athena-results/",
        ctas_approach=False,  # must set to False to handle timestamp of Iceberg table
    )

    if df.empty:
        return []

    # Convert DataFrame rows to Post objects
    posts = []
    for row in df.to_dict(orient="records"):
        post = Post.from_dict(row)
        posts.append(post)

    return posts


def save_sentiments(
    sentiments: list[Sentiment], min_created_at: int, max_created_at: int
):
    """
    Save sentiments to the post table
    """
    table = "post"
    database = "sentimental"
    bucket = os.environ["S3_BUCKET_NAME"]

    query_executions = []

    for sentiment in sentiments:
        execution: str = wr.athena.start_query_execution(
            sql=f"""
            UPDATE {database}.{table}
            SET sentiment = '{sentiment.sentiment}',
                sentiment_score_mixed = {sentiment.mixed},
                sentiment_score_positive = {sentiment.positive},
                sentiment_score_negative = {sentiment.negative},
                sentiment_score_neutral = {sentiment.neutral}
            WHERE id = '{sentiment.post.id}'
                AND created_at BETWEEN from_unixtime({min_created_at})
                AND from_unixtime({max_created_at})
            """,
            database=database,
            s3_output=f"s3://{bucket}/athena-results/",
            wait=False,
        )
        query_executions.append(execution)

    for execution in query_executions:
        wr.athena.wait_query(execution)
