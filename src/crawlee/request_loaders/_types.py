from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Literal

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from datetime import datetime, timedelta


@docs_group('Request loaders')
@dataclass(frozen=True)
class ReadyStatus:
    """The next `fetch_next_request` call is expected to return a request."""

    status: ClassVar[Literal['ready']] = 'ready'
    """The status name, so a check doesn't need to import the class."""


@docs_group('Request loaders')
@dataclass(frozen=True, kw_only=True)
class WaitingStatus:
    """Nothing to fetch right now, but the source isn't done.

    Requests are in progress, are being added in the background, or are held back until `ready_at`.
    """

    status: ClassVar[Literal['waiting']] = 'waiting'
    """The status name, so a check doesn't need to import the class."""

    ready_at: datetime | None = None
    """When the source expects to become ready, as a timezone-aware UTC time. It may become ready earlier, so a consumer
    keeps polling either way. `None` when the wait has no clock.
    """


@docs_group('Request loaders')
@dataclass(frozen=True)
class StalledStatus:
    """The source holds requests it can't make progress on.

    Only a manager that paces its own dispatch, or one that wraps such a manager, reports it.
    """

    status: ClassVar[Literal['stalled']] = 'stalled'
    """The status name, so a check doesn't need to import the class."""

    reason: str
    """Why the source can't make progress."""


@docs_group('Request loaders')
@dataclass(frozen=True)
class FinishedStatus:
    """Every request has been handled."""

    status: ClassVar[Literal['finished']] = 'finished'
    """The status name, so a check doesn't need to import the class."""


RequestLoaderStatus = ReadyStatus | WaitingStatus | FinishedStatus
"""Readiness of a request source that never stalls, such as `RequestList`, `SitemapRequestLoader` or `RequestQueue`."""

RequestSourceStatus = ReadyStatus | WaitingStatus | StalledStatus | FinishedStatus
"""Readiness of a request source, as reported by `RequestLoader.check_readiness`."""


PacingScope = str
"""How much of the URL space a pacing signal covers.

The vocabulary is open, so a manager that paces by something else can be reported to in its own terms. Crawlee itself
reports `'hostname'`, and `'registrable_domain'` names a whole site with its subdomains. `ThrottlingRequestManager`
accepts only `'hostname'`.
"""


@docs_group('Request loaders')
@dataclass(frozen=True, kw_only=True)
class RateLimitedSignal:
    """The source turned a request away because the crawler was going too fast, such as with an HTTP 429."""

    reason: ClassVar[Literal['rate_limited']] = 'rate_limited'
    """The signal name, so a check doesn't need to import the class."""

    url: str = field(kw_only=False)
    """The URL that was turned away."""

    wait: timedelta | None = None
    """How long the source asked to wait before trying again, if it said."""

    scope: PacingScope | None = None
    """How far the refusal reaches, if the reporter can tell. `None` lets the manager apply it however it groups
    requests.
    """


@docs_group('Request loaders')
@dataclass(frozen=True, kw_only=True)
class MinIntervalSignal:
    """The source declared a minimum interval between requests, such as a robots.txt `Crawl-delay`."""

    reason: ClassVar[Literal['min_interval']] = 'min_interval'
    """The signal name, so a check doesn't need to import the class."""

    url: str = field(kw_only=False)
    """A URL of the source that declared the interval."""

    interval: timedelta
    """The declared minimum interval between two requests to the source."""

    scope: PacingScope
    """How much of the URL space the interval applies to."""


PacingSignal = RateLimitedSignal | MinIntervalSignal
"""A signal about the pace at which requests may go out, as reported by `RequestManager.record_pacing_signal`."""
