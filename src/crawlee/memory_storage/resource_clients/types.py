from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime


@dataclass
class ResourceInfo:
    """Resource information."""

    accessed_at: datetime
    created_at: datetime
    had_multiple_clients: bool
    handled_request_count: int
    id: str
    modified_at: datetime
    name: str | None
    pending_request_count: int
    stats: dict[str, Any]
    total_request_count: int
    user_id: str
    resource_directory: str
