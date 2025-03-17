"""Unit tests for ChatGPTProvider class."""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import json
import os

from providers.chatgpt_provider import ChatGPTProvider
from model.post import Post
from model.sentiment import Sentiment
from model.job import Job
from model.job import ChatGPTProviderData


class TestChatGPTProvider(unittest.TestCase):
    """Unit tests for ChatGPTProvider class."""

    def setUp(self):
        """Set up test fixtures."""
        self.provider = ChatGPTProvider()
        self.sample_post = Post(
            id="123",
            keyword="test keyword",
            source="reddit",
            title="Test Title",
            created_at=datetime(2024, 1, 1),
            body="Test body content",
            comments=["Test comment 1", "Test comment 2"],
            post_url="https://example.com/test",
        )

        # Mock environment variables
        os.environ["JOBS_TABLE_NAME"] = "test-jobs-table"
        os.environ["OPENAI_API_KEY"] = "test-api-key"
        os.environ["S3_BUCKET_NAME"] = "test-bucket"

    def test_get_provider_name(self):
        """Test get_provider_name returns correct value."""
        self.assertEqual(self.provider.get_provider_name(), "chatgpt")

    def test_create_sentiment_job_empty_posts(self):
        """Test create_sentiment_job with empty posts list."""
        with self.assertRaises(ValueError):
            self.provider.create_sentiment_job([], "test-job")

    @patch("openai.batches.retrieve")
    def test_check_job_status_completed(self, mock_batch_retrieve):
        """Test check_job_status when job is completed."""
        # Create a job
        job = Job(
            job_id="test-job-123",
            job_name="Test Job",
            status="SUBMITTED",
            created_at=datetime.now(),
            posts=[self.sample_post],
            provider="chatgpt",
            provider_data=ChatGPTProviderData(
                openai_batch_id="batch-123", output_file_id=None
            ),
        )

        # Mock OpenAI response for completed job
        mock_batch_retrieve.return_value = MagicMock(
            status="completed", output_file_id="output-123"
        )

        self.provider.query_and_update_job(job)

        self.assertEqual(job.status, "COMPLETED")
        self.assertEqual(job.provider_data.output_file_id, "output-123")

    @patch("openai.batches.retrieve")
    def test_check_job_status_failed(self, mock_batch_retrieve):
        """Test check_job_status when job has failed."""
        job = Job(
            job_id="test-job-123",
            job_name="Test Job",
            status="SUBMITTED",
            created_at=datetime.now(),
            posts=[self.sample_post],
            provider="chatgpt",
            provider_data=ChatGPTProviderData(
                openai_batch_id="batch-123",
                output_file_id=None,
                error_file_id="error-123",
            ),
        )

        # Mock OpenAI response for failed job
        mock_batch_retrieve.return_value = MagicMock(status="failed")

        self.provider.query_and_update_job(job)
        self.assertEqual(job.status, "FAILED")
        self.assertEqual(job.provider_data.error_file_id, "error-123")

    @patch("openai.files.content")
    def test_process_completed_job(self, mock_file_content):
        """Test process_completed_job with valid output."""
        # Create a job with output file ID
        job = Job(
            job_id="test-job-123",
            job_name="Test Job",
            status="COMPLETED",
            created_at=datetime.now(),
            posts=[self.sample_post],
            provider="chatgpt",
            provider_data=ChatGPTProviderData(
                openai_batch_id="batch-123", output_file_id="output-123"
            ),
        )

        # Mock OpenAI file content response
        mock_content = MagicMock()
        mock_content.text = json.dumps(
            {
                "custom_id": "123",
                "response": {
                    "status_code": 200,
                    "body": {
                        "choices": [
                            {
                                "message": {
                                    "content": json.dumps(
                                        {
                                            "sentiment": "POSITIVE",
                                            "scores": {
                                                "Mixed": 0.1,
                                                "Positive": 0.7,
                                                "Negative": 0.1,
                                                "Neutral": 0.1,
                                            },
                                        }
                                    )
                                }
                            }
                        ]
                    },
                },
            }
        )
        mock_file_content.return_value = mock_content

        # Process the job
        sentiments = self.provider.process_completed_job(job)

        # Verify results
        self.assertEqual(len(sentiments), 1)
        sentiment = sentiments[0]
        self.assertIsInstance(sentiment, Sentiment)
        self.assertEqual(sentiment.sentiment, "POSITIVE")
        self.assertEqual(sentiment.positive, 0.7)
        self.assertEqual(sentiment.negative, 0.1)
        self.assertEqual(sentiment.neutral, 0.1)
        self.assertEqual(sentiment.mixed, 0.1)
        self.assertEqual(sentiment.post.id, self.sample_post.id)

    @patch("openai.files.content")
    @patch("boto3.client")
    def test_process_completed_job_no_output_file(self, mock_boto3_client, mock_file_content):
        """Test process_completed_job when output_file_id is not set."""
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        # Mock OpenAI error file content
        mock_content = MagicMock()
        mock_content.text = "Error details"  # Use text property instead of read()
        mock_file_content.return_value = mock_content

        job = Job(
            job_id="test-job-123",
            job_name="Test Job",
            status="COMPLETED",
            created_at=datetime.now(),
            posts=[self.sample_post],
            provider="chatgpt",
            provider_data=ChatGPTProviderData(
                openai_batch_id="batch-123",
                output_file_id=None,
                error_file_id="error-123",
            ),
        )

        # Process the job and verify empty result for error case
        result = self.provider.process_completed_job(job)
        self.assertEqual(result, [])

        # Verify S3 was called with correct parameters
        mock_boto3_client.assert_called_once_with("s3")
        mock_file_content.assert_called_once_with("error-123")

        # Verify S3 put_object was called with correct parameters
        mock_s3.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="chatgpt/jobs/test-job-123/error.jsonl",
            Body=mock_content.text,  # Use the mock encoded value
            ContentType="text/plain",
        )


if __name__ == "__main__":
    unittest.main()
