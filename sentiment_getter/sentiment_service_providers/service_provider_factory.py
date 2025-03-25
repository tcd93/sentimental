"""
Factory for creating sentiment providers.
"""

import os
import logging

from sentiment_service_providers.sentiment_service_provider import SentimentServiceProvider
from sentiment_service_providers.comprehend_provider import ComprehendProvider
from sentiment_service_providers.chatgpt_provider import ChatGPTProvider


def get_service_provider(
    logger: logging.Logger | None = None, provider_name: str | None = None
) -> SentimentServiceProvider:
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

    if provider_name is not None:
        if provider_name == "chatgpt":
            return ChatGPTProvider(logger)
        if provider_name == "comprehend":
            return ComprehendProvider(logger)
        raise ValueError(f"Invalid provider name: {provider_name}")

    # if OPENAI_API_KEY is set, use ChatGPT provider
    if os.environ.get("OPENAI_API_KEY"):
        return ChatGPTProvider(logger)

    return ComprehendProvider(logger)
