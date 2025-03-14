"""Unit tests for Steam scrapper functionality."""

import unittest
import os
from unittest.mock import patch, MagicMock
from datetime import datetime
from functions.steam.scraper import get_app_id, get_steam_reviews
from model.post import Post


class TestSteamScrapper(unittest.TestCase):
    """Unit tests for Steam scrapper functionality."""

    @patch("functions.steam.scraper.requests.get")
    def test_get_app_id(self, mock_get):
        """Test that the get_app_id function returns a valid app ID."""
        # Mock the response from Steam API
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "total": 1,
            "items": [{"id": 570, "name": "Dota 2", "price": "Free to Play"}],
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Test the function
        result = get_app_id("Dota 2")

        # Verify the result
        self.assertEqual(result, 570)
        mock_get.assert_called_once()

        # Test with no results
        mock_response.json.return_value = {"total": 0, "items": []}
        result = get_app_id("NonexistentGame12345")
        self.assertIsNone(result)

    @patch("functions.steam.scraper.get_app_id")
    @patch("functions.steam.scraper.requests.get")
    def test_get_steam_reviews(self, mock_get, mock_get_app_id):
        """Test that the get_steam_reviews function returns a list of posts."""
        # Mock the app ID
        mock_get_app_id.return_value = 570  # Dota 2

        # Mock the response from Steam API
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "success": 1,
            "reviews": [
                {
                    "recommendationid": "123456789",
                    "author": {
                        "steamid": "76561198123456789",
                        "num_games_owned": 100,
                        "num_reviews": 10,
                    },
                    "review": "This is a test review for Dota 2",
                    "timestamp_created": 1609459200,  # 2021-01-01
                    "votes_up": 42,
                },
                {
                    "recommendationid": "987654321",
                    "author": {
                        "steamid": "76561198987654321",
                        "num_games_owned": 200,
                        "num_reviews": 20,
                    },
                    "review": "Another test review for Dota 2",
                    "timestamp_created": 1609545600,  # 2021-01-02
                    "votes_up": 21,
                },
            ],
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Test the function
        result = get_steam_reviews(
            keyword="Dota 2", time_filter="day", sort="top", post_limit=2
        )

        # Verify the result
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

        # Check if all elements are Post objects with correct data
        for post in result:
            self.assertIsInstance(post, Post)
            self.assertTrue(post.id.startswith("steam_"))
            self.assertTrue("Steam Review for Dota 2" in post.title)
            self.assertIsInstance(post.created_at, datetime)
            self.assertIsInstance(post.body, str)
            self.assertIsInstance(post.comments, list)

        # Test with no app ID
        mock_get_app_id.return_value = None
        result = get_steam_reviews(keyword="NonexistentGame12345")
        self.assertEqual(result, [])

        # Test with API error
        mock_get_app_id.return_value = 570
        mock_response.json.return_value = {"success": 0}
        result = get_steam_reviews(keyword="Dota 2")
        self.assertEqual(result, [])

    # Real API tests
    def test_real_get_app_id(self):
        """Test getting a real app ID from Steam."""
        # Skip this test if SKIP_REAL_API_TESTS environment variable is set
        if os.environ.get("SKIP_REAL_API_TESTS"):
            self.skipTest("Skipping real API test")

        # Test with a popular game that should exist
        app_id = get_app_id("Counter-Strike")
        self.assertIsNotNone(app_id)
        self.assertIsInstance(app_id, int)

        # Test with a game that shouldn't exist
        app_id = get_app_id("ThisGameDefinitelyDoesNotExist12345")
        self.assertIsNone(app_id)

    def test_real_get_steam_reviews(self):
        """Test getting real reviews from Steam."""
        # Skip this test if SKIP_REAL_API_TESTS environment variable is set
        if os.environ.get("SKIP_REAL_API_TESTS"):
            self.skipTest("Skipping real API test")

        # Test with a popular game
        posts = get_steam_reviews(
            keyword="Counter-Strike",
            time_filter="all",  # Use "all" to ensure we get some reviews
            sort="top",
            post_limit=3,
        )

        # Verify we got some posts
        self.assertIsInstance(posts, list)
        self.assertGreater(len(posts), 0)

        # Check post structure
        for post in posts:
            self.assertIsInstance(post, Post)
            self.assertTrue(post.id.startswith("steam_"))
            self.assertIsInstance(post.title, str)
            self.assertIsInstance(post.body, str)
            self.assertIsInstance(post.created_at, datetime)
            self.assertIsInstance(post.comments, list)

        # Test with a non-existent game
        posts = get_steam_reviews(keyword="ThisGameDefinitelyDoesNotExist12345")
        self.assertEqual(posts, [])


if __name__ == "__main__":
    unittest.main()
