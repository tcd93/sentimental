"""Unit tests for Reddit scrapper functionality."""

import unittest
import os
from dotenv import load_dotenv
from functions.scrapers.reddit.scrapper import (
    get_reddit_posts,
    get_subreddits_from_chatgpt,
    get_openai_client,
)
from model.post import Post


class TestRedditScrapper(unittest.TestCase):
    """Unit tests for Reddit scrapper functionality."""

    def setUp(self):
        # Load environment variables from .env file
        load_dotenv()

        # Check if OPENAI_API_KEY is set
        self.assertTrue(
            "OPENAI_API_KEY" in os.environ,
            "OPENAI_API_KEY not found in .env file (project root)",
        )
        self.assertTrue(
            "REDDIT_CLIENT_ID" in os.environ,
            "REDDIT_CLIENT_ID not found in .env file (project root)",
        )
        self.assertTrue(
            "REDDIT_CLIENT_SECRET" in os.environ,
            "REDDIT_CLIENT_SECRET not found in .env file (project root)",
        )
        # Save environment variables
        self.env_vars = {}
        for key in ["OPENAI_API_KEY"]:
            if key in os.environ:
                self.env_vars[key] = os.environ[key]

    def tearDown(self):
        # Restore environment variables
        for key, value in self.env_vars.items():
            os.environ[key] = value

        # Remove any environment variables that were added during tests
        for key in ["OPENAI_API_KEY"]:
            if key not in self.env_vars and key in os.environ:
                del os.environ[key]

    # real test
    def test_get_subreddits_from_chatgpt(self):
        """Test that the get_subreddits_from_chatgpt function returns a list of 3 subreddits."""
        openai = get_openai_client()
        result = get_subreddits_from_chatgpt(keyword="league of legends", openai=openai)
        # check if result is a list and has 3 elements
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)
        # check if all elements are strings and does not contain "r/"
        for subreddit in result:
            self.assertIsInstance(subreddit, str)
            self.assertNotIn("r/", subreddit)

    def test_get_reddit_posts(self):
        """Test that the get_reddit_posts function returns a list of posts."""
        result = get_reddit_posts(
            keyword="What",
            subreddits=["askreddit"],
            time_filter="week",
            sort="top",
            post_limit=3,
            top_comments_limit=5,
        )
        # check if result is a list and has 3 elements
        self.assertIsInstance(result, list)
        # well if r/askreddit returns less than 3 posts for a week, then you're probably
        # reading this code in 2077
        self.assertEqual(len(result), 3)
        # check if all elements are Post objects
        for post in result:
            self.assertIsInstance(post, Post)


if __name__ == "__main__":
    unittest.main()
