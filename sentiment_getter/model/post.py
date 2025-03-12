"""
This module contains the Post class, which is used to represent a post on a social media platform.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Post:
    """Post object"""

    id: str
    title: str
    created_at: datetime
    comments: list[str]

    def get_comment_text(self) -> str:
        """Return all comments joined as a single string."""
        return "\n".join(self.comments)
