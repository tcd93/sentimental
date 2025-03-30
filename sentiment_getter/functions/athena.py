"""Common function"""

import logging
import os
import re

from models.post import Post
from models.sentiment import Sentiment
import awswrangler as wr
import pandas as pd


def sanitize_for_athena(name):
    """Sanitize a string for use as an Athena table name."""
    # Replace special characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)

    # Ensure it starts with a letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"

    return sanitized.lower()


def get_posts(execution_id: str) -> list[Post]:
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
    """
    # Execute Athena query and get results as pandas DataFrame
    df: pd.DataFrame = wr.athena.read_sql_query(
        sql=query,
        database=database,
        s3_output=f"s3://{bucket}/athena-results/",
        ctas_approach=False,  # must set to False if we have timestamp in filter
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
    job_id: str,
    sentiments: list[Sentiment],
    min_created_at: int,
    max_created_at: int,
):
    """
    Merge sentiments to the sentiment table (on post_id)

    Args:
        job_id: Job ID
        sentiments: List of Sentiment objects to save
        min_created_at: Minimum created_at timestamp
        max_created_at: Maximum created_at timestamp
    """
    table = "sentiment"
    # Sanitize the job_id to create a valid table name
    sanitized_job_id = sanitize_for_athena(job_id)
    temp_table = f"temp_sentiment_{sanitized_job_id}"
    database = "sentimental"
    bucket = os.environ["S3_BUCKET_NAME"]

    # Convert list of Sentiment objects to DataFrame for batch processing
    sentiment_dicts = [sentiment.to_dict() for sentiment in sentiments]
    if not sentiment_dicts:
        return  # No sentiments to save

    df_sentiments = pd.DataFrame(sentiment_dicts)

    # Write the DataFrame to S3 and create a temp table
    s3_temp_path = f"s3://{bucket}/temp_data/{temp_table}/"
    wr.s3.to_parquet(
        df=df_sentiments,
        path=s3_temp_path,
        dataset=True,
        mode="overwrite",
    )

    # Create temp table from the S3 data
    athena_type_mapping = {
        "keyword": "STRING",
        "created_at": "TIMESTAMP",
        "execution_id": "STRING",
        "post_id": "STRING",
        "post_url": "STRING",
        "sentiment": "STRING",
        "sentiment_score_mixed": "DOUBLE",
        "sentiment_score_positive": "DOUBLE",
        "sentiment_score_neutral": "DOUBLE",
        "sentiment_score_negative": "DOUBLE",
    }

    # First delete any existing temp table
    wr.catalog.delete_table_if_exists(database=database, table=temp_table)

    # Create the temp table with Parquet format
    wr.catalog.create_parquet_table(
        database=database,
        table=temp_table,
        path=s3_temp_path,
        columns_types=athena_type_mapping,
        mode="overwrite",
    )

    # Perform a MERGE operation using Iceberg's MERGE INTO syntax
    merge_query = f"""
    MERGE INTO {database}.{table} t
    USING {database}.{temp_table} s
    ON t.post_id = s.post_id 
    AND t.created_at BETWEEN from_unixtime({min_created_at}) AND from_unixtime({max_created_at})
    WHEN MATCHED
      THEN UPDATE SET
        keyword = s.keyword,
        created_at = s.created_at,
        execution_id = s.execution_id,
        post_id = s.post_id,
        post_url = s.post_url,
        sentiment = s.sentiment,
        sentiment_score_mixed = s.sentiment_score_mixed,
        sentiment_score_positive = s.sentiment_score_positive,
        sentiment_score_neutral = s.sentiment_score_neutral,
        sentiment_score_negative = s.sentiment_score_negative
    WHEN NOT MATCHED
      THEN INSERT VALUES (
        s.keyword,
        s.created_at,
        s.execution_id,
        s.post_id,
        s.post_url,
        s.sentiment,
        s.sentiment_score_mixed,
        s.sentiment_score_positive,
        s.sentiment_score_neutral,
        s.sentiment_score_negative
      )
    """
    logger = logging.getLogger()
    logger.info("Merge query: \n%s", merge_query)
    # Execute the merge operation
    response = wr.athena.start_query_execution(
        sql=merge_query,
        database=database,
        s3_output=f"s3://{bucket}/athena-results/",
        wait=True,
    )
    logger.info("Merge operation response: \n%s", response)
    if response["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(
            f"Merge operation failed: {response["Status"]["StateChangeReason"]}"
        )

    # Clean up temp table
    wr.catalog.delete_table_if_exists(database=database, table=temp_table)

    # Clean up S3 temp data
    wr.s3.delete_objects(s3_temp_path)

    return response.get("QueryExecutionId")
