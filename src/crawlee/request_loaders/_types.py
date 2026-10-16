from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from datetime import datetime


@docs_group('Request loaders')
@dataclass(frozen=True)
class ReadyStatus:
    """The next `fetch_next_request` call is expected to return a request."""


@docs_group('Request loaders')
@dataclass(frozen=True, kw_only=True)
class WaitingStatus:
    """Nothing to fetch right now, but the source isn't done.

    Requests are in progress, are being added in the background, or are held back until `ready_at`.
    """

    ready_at: datetime | None = None
    """When the source expects to become ready, as a timezone-aware UTC time. `None` when the wait has no clock."""


@docs_group('Request loaders')
@dataclass(frozen=True)
class StalledStatus:
    """The source holds requests it can't make progress on. Only a manager that paces its own dispatch reports it."""

    reason: str
    """Why the source can't make progress."""


@docs_group('Request loaders')
@dataclass(frozen=True)
class FinishedStatus:
    """Every request has been handled."""


RequestLoaderStatus = ReadyStatus | WaitingStatus | FinishedStatus
"""Readiness of a request loader. Loaders never stall."""

RequestSourceStatus = ReadyStatus | WaitingStatus | StalledStatus | FinishedStatus
"""Readiness of a request source, as reported by `RequestLoader.check_readiness`."""
