"""Unit tests for ComprehendProvider class."""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import json
import os
import io
import tarfile

from sentiment_service_providers.comprehend_provider import ComprehendProvider
from models.post import Post
from models.job import Job


class TestComprehendProvider(unittest.TestCase):
    """Unit tests for ComprehendProvider class."""

    def setUp(self):
        """Set up test fixtures."""
        self.provider = ComprehendProvider()
        self.sample_post = Post(
            id="123",
            execution_id="123",
            keyword="test keyword",
            source="reddit",
            title="Test Title",
            created_at=datetime(2024, 1, 1),
            body="Test body content",
            comments=["Test comment 1", "Test comment 2"],
            post_url="https://example.com/test",
        )
        self.sample_post2 = Post(
            id="123",
            keyword="test keyword",
            source="reddit",
            title="Test Title 2",
            created_at=datetime(2024, 1, 1),
            body="Test body content 2",
            comments=["Test comment 1", "Test comment 2"],
            post_url="https://example.com/test2",
        )

        # Mock environment variables
        os.environ["S3_BUCKET_NAME"] = "test-bucket"
        os.environ["COMPREHEND_ROLE_ARN"] = "test-role-arn"

    def test_get_provider_name(self):
        """Test get_provider_name returns correct value."""
        self.assertEqual(self.provider.get_provider_name(), "comprehend")

    def test_create_sentiment_job_empty_posts(self):
        """Test create_sentiment_job with empty posts list."""
        result = self.provider.create_sentiment_job([], "test-job", "test-execution-id")
        self.assertEqual(result, {"error": "No posts to analyze"})

    @patch("boto3.client")
    def test_create_sentiment_job_success(self, mock_boto3_client):
        """Test create_sentiment_job with valid posts."""
        # Mock S3 and Comprehend clients
        mock_s3 = MagicMock()
        mock_comprehend = MagicMock()
        mock_boto3_client.side_effect = [mock_s3, mock_comprehend]

        # Mock Comprehend response
        mock_comprehend.start_sentiment_detection_job.return_value = {
            "JobId": "test-job-123"
        }

        # Create job
        job = self.provider.create_sentiment_job(
            [self.sample_post, self.sample_post2], "test-job", "test-execution-id"
        )

        # Verify S3 upload
        mock_s3.put_object.assert_called_once()
        self.assertEqual(job.job_id, "test-job-123")
        self.assertEqual(job.status, "SUBMITTED")
        self.assertEqual(job.provider, "comprehend")

    @patch("boto3.client")
    def test_query_and_update_job_completed(self, mock_boto3_client):
        """Test query_and_update_job when job is completed."""
        mock_comprehend = MagicMock()
        mock_boto3_client.return_value = mock_comprehend

        # Mock Comprehend response for completed job
        mock_comprehend.describe_sentiment_detection_job.return_value = {
            "SentimentDetectionJobProperties": {"JobStatus": "COMPLETED"}
        }

        job = Job(
            job_id="test-job-123",
            job_name="Test Job",
            status="SUBMITTED",
            created_at=datetime.now(),
            post_keys=[self.sample_post.get_s3_key(), self.sample_post2.get_s3_key()],
            provider="comprehend",
        )

        result = self.provider.query_and_update_job(job)
        self.assertTrue(result)
        self.assertEqual(job.status, "COMPLETED")

    @patch("boto3.client")
    def test_process_completed_job(self, mock_boto3_client):
        """Test process_completed_job with valid output."""
        mock_s3 = MagicMock()
        mock_comprehend = MagicMock()
        mock_boto3_client.side_effect = [mock_s3, mock_comprehend]

        # Create mock tar file with results
        sentiment_result = {
            "Sentiment": "POSITIVE",
            "SentimentScore": {
                "Mixed": 0.1,
                "Positive": 0.7,
                "Negative": 0.1,
                "Neutral": 0.1,
            },
        }

        # Create tar file in memory with two lines of sentiment results
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
            data = json.dumps(sentiment_result).encode("utf-8")
            info = tarfile.TarInfo(name="output")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
            data = json.dumps(sentiment_result).encode("utf-8")
            info = tarfile.TarInfo(name="output")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        # Mock S3 and Comprehend responses
        mock_comprehend.describe_sentiment_detection_job.return_value = {
            "SentimentDetectionJobProperties": {
                "OutputDataConfig": {"S3Uri": "s3://test-bucket/output/test-job"}
            }
        }

        mock_body = MagicMock()
        mock_body.read.return_value = tar_buffer.getvalue()
        mock_s3.get_object.return_value = {"Body": mock_body}

        job = Job(
            job_id="test-job-123",
            job_name="Test Job",
            status="COMPLETED",
            created_at=datetime.now(),
            post_keys=[self.sample_post.get_s3_key(), self.sample_post2.get_s3_key()],
            provider="comprehend",
        )

        sentiments = self.provider.process_completed_job(job, [self.sample_post, self.sample_post2])

        self.assertEqual(len(sentiments), 2)
        sentiment = sentiments[0]
        self.assertEqual(sentiment.sentiment, "POSITIVE")
        self.assertEqual(sentiment.positive, 0.7)
        self.assertEqual(sentiment.negative, 0.1)
        self.assertEqual(sentiment.neutral, 0.1)
        self.assertEqual(sentiment.mixed, 0.1)
        self.assertEqual(sentiment.post.id, self.sample_post.id)


if __name__ == "__main__":
    unittest.main()
