"""
Factory for creating sentiment providers.
"""

import os
import logging

from functions.job_creator.sentiment_provider import SentimentProvider
from functions.job_creator.comprehend_provider import ComprehendProvider
from functions.job_creator.chatgpt_provider import ChatGPTProvider

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_provider() -> SentimentProvider:
    """
    Get a sentiment provider instance.

    Args:
        provider_name: Name of the provider to use, or None to use default

    Returns:
        SentimentProvider instance
    """
    # if OPENAI_API_KEY is set, use ChatGPT provider
    if os.environ.get("OPENAI_API_KEY"):
        logger.info("Using ChatGPT provider")
        return ChatGPTProvider()
    logger.info("Using Comprehend provider")
    return ComprehendProvider()