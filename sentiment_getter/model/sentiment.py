"""
This module contains the Sentiment class, which is used to represent the sentiment of a post.
"""

import os
from dataclasses import dataclass
import json
import logging
from model.job import Job
from model.post import Post
from supabase import create_client, Client

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class Sentiment:
    """Sentiment for a post."""

    job: Job
    post: Post
    sentiment: str
    mixed: float
    positive: float
    negative: float
    neutral: float

    def sync_supabase(self):
        """Sync the sentiment to Supabase. Returns the number of records upserted."""

        # Initialize Supabase client
        supabase: Client = create_client(
            os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"]
        )

        record = {
            "keyword": self.post.keyword,
            "source": self.post.source,
            "post_created_time": self.post.created_at.isoformat(),
            "post_id": self.post.id,
            "post_url": self.post.post_url,
            "sentiment": self.sentiment,
            "sentiment_score_mixed": self.mixed,
            "sentiment_score_positive": self.positive,
            "sentiment_score_neutral": self.neutral,
            "sentiment_score_negative": self.negative,
            "job_id": self.job.job_id,
        }
        logger.debug("Upserting record: %s", json.dumps(record, indent=4))

        result = (
            supabase.table("sentiment_results")
            .upsert(record, on_conflict="post_id,job_id", count="exact")
            .execute()
        )
        if result.count is not None:
            logger.debug("Successfully upserted %d records in Supabase", result.count)
            return result.count

        return len(result.data)
