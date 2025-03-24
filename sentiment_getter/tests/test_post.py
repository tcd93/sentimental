"""Unit tests for Post model."""

import unittest
from datetime import datetime
import logging

from model.post import Post


class TestPost(unittest.TestCase):
    """Unit tests for Post model."""

    def setUp(self):
        """Set up test fixtures."""
        self.logger = logging.getLogger("test")
        self.logger.setLevel(logging.INFO)

        # Create a sample post for testing
        self.post = Post(
            id="test-post-123",
            keyword="test keyword",
            source="reddit",
            title="Test Post Title\nwith newline",
            created_at=datetime(2023, 3, 14, 15, 30),
            body="This is a test post body.\nIt has multiple lines.\nTesting newline replacement.",
            comments=[
                "First comment\nwith newline",
                "Second comment\nalso with newline",
                "Third comment",
            ],
            post_url="https://example.com/test-post",
            logger=self.logger,
            execution_id="test-execution-123",
        )

    def test_get_text(self):
        """Test the get_text method properly formats post content."""
        expected_text = (
            "title: Test Post Title.with newline; "
            "body: This is a test post body..It has multiple lines..Testing newline replacement.; "
            "comments: First comment.with newline - Second comment.also with newline - Third comment"
        )

        result = self.post.get_text()
        self.assertEqual(result, expected_text)

    def test_get_text_empty_fields(self):
        """Test the get_text method with empty fields."""
        post_with_empty = Post(
            id="empty-post",
            keyword="empty",
            source="test",
            title="",
            created_at=datetime(2023, 3, 14, 15, 30),
            body="",
            comments=[],
            logger=self.logger,
        )

        expected_text = "title: ; body: ; comments: "
        result = post_with_empty.get_text()
        self.assertEqual(result, expected_text)


if __name__ == "__main__":
    unittest.main()
