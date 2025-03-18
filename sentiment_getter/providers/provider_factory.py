"""
Factory for creating sentiment providers.
"""

import os
import logging

from providers.sentiment_provider import SentimentProvider
from providers.comprehend_provider import ComprehendProvider
from providers.chatgpt_provider import ChatGPTProvider

def get_provider(logger: logging.Logger | None = None) -> SentimentProvider:
    """
    Get a sentiment provider instance.

    Args:
        provider_name: Name of the provider to use, or None to use default

    Returns:
        SentimentProvider instance
    """
    if logger is None:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

    # if OPENAI_API_KEY is set, use ChatGPT provider
    if os.environ.get("OPENAI_API_KEY"):
        return ChatGPTProvider(logger)
    return ComprehendProvider(logger)
