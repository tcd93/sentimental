"""Unit tests for provider factory."""

import unittest
from unittest.mock import patch
import os
import logging

from sentiment_service_providers.service_provider_factory import get_service_provider
from sentiment_service_providers.chatgpt_provider import ChatGPTProvider

class TestProviderFactory(unittest.TestCase):
    """Unit tests for provider factory."""

    def setUp(self):
        """Set up test fixtures."""
        self.logger = logging.getLogger("test")
        self.logger.setLevel(logging.INFO)

    @patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"})
    def test_get_provider_chatgpt(self):
        """Test get_provider returns ChatGPTProvider when OPENAI_API_KEY is set."""
        provider = get_service_provider(self.logger)
        self.assertIsInstance(provider, ChatGPTProvider)
        self.assertEqual(provider.logger, self.logger)


if __name__ == "__main__":
    unittest.main()
