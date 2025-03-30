"""
Model data for sentiment analysis jobs. Keep small to pass between Step Function states.
"""

from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class ChatGPTProviderData:
    """Provider-specific data for ChatGPT."""

    openai_batch_id: str
    """The ID of the batch job created by OpenAI."""

    output_file_id: str | None = None
    """The ID of the output file created by OpenAI (when the job is completed)."""

    error_file_id: str | None = None
    """The ID of the error file created by OpenAI (when the job fails)."""

    def to_dict(self) -> dict[str, any]:
        """Convert the ChatGPTProviderData object to a dictionary."""
        return {
            "openai_batch_id": self.openai_batch_id,
            "output_file_id": self.output_file_id,
            "error_file_id": self.error_file_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, any]) -> "ChatGPTProviderData":
        """Convert a dictionary to a ChatGPTProviderData object."""
        return cls(
            openai_batch_id=data["openai_batch_id"],
            output_file_id=data.get("output_file_id"),
            error_file_id=data.get("error_file_id"),
        )


@dataclass(frozen=True)
class ComprehendProviderData:
    """Provider-specific data for Comprehend."""

    @classmethod
    def from_dict(cls, _: dict[str, any]) -> "ComprehendProviderData":
        """Convert a dictionary to a ComprehendProviderData object."""
        return cls()


@dataclass(frozen=True)
class Job:
    """Represents a sentiment analysis job."""

    job_id: str
    job_name: str
    status: str
    created_at: datetime
    post_ids: list[str]
    provider: str
    provider_data: ChatGPTProviderData | ComprehendProviderData = None

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)

    @classmethod
    def from_dict(cls, data: dict[str, any]) -> "Job":
        """
        Create a Job object from job metadata (exported from `to_dict_minimal`).
        """
        if data.get("provider") == "chatgpt":
            provider_data = ChatGPTProviderData.from_dict(data["provider_data"])
        elif data.get("provider") == "comprehend":
            provider_data = ComprehendProviderData.from_dict(data["provider_data"])
        else:
            raise ValueError(f"Unsupported provider: {data['provider']}")

        return cls(
            job_id=data["job_id"],
            job_name=data["job_name"],
            status=data["status"],
            created_at=(
                datetime.fromisoformat(data["created_at"])
                if isinstance(data["created_at"], str)
                else data["created_at"]
            ),
            post_ids=data["post_ids"],
            provider=data["provider"],
            provider_data=provider_data,
        )

    def to_dict(self) -> dict[str, any]:
        """
        Convert the Job object to a dictionary.

        Returns:
            Dictionary representation of the Job
        """
        result = {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "status": self.status,
            "created_at": (
                self.created_at.isoformat()
                if isinstance(self.created_at, datetime)
                else self.created_at
            ),
            "post_ids": self.post_ids,
            "provider": self.provider,
            "provider_data": (
                self.provider_data.to_dict() if self.provider_data else None
            ),
        }

        return result
