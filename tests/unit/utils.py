from __future__ import annotations

import asyncio
import inspect
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from crawlee.http_clients._base import HttpClient, HttpResponse
from crawlee.request_loaders import ThrottlingRequestManager, _throttling_request_manager
from crawlee.storages import RequestQueue

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from yarl import URL

    from crawlee import Request
    from crawlee.storage_clients.models import ProcessedRequest

T = TypeVar('T')

run_alone_on_mac = pytest.mark.run_alone if sys.platform == 'darwin' else lambda x: x


def make_status_stream_client(
    responses: dict[str, list[tuple[int, bytes] | Exception]],
) -> tuple[AsyncMock, list[int]]:
    """Create a mock client that answers each URL with its next `(status, body)` response or raised exception.

    The last entry of a URL's sequence repeats for any further requests. The returned list records the status of
    every response served (exception entries are not recorded).
    """
    attempts: list[int] = []
    indexes: dict[str, int] = {}

    @asynccontextmanager
    async def stream(url: str, **_kwargs: Any) -> AsyncIterator[HttpResponse]:
        sequence = responses[url]
        index = indexes.get(url, 0)
        indexes[url] = index + 1
        spec = sequence[min(index, len(sequence) - 1)]
        if isinstance(spec, Exception):
            raise spec

        status, body = spec
        attempts.append(status)

        async def read_stream() -> AsyncIterator[bytes]:
            if body:
                yield body

        response = MagicMock(spec=HttpResponse)
        response.status_code = status
        response.headers = {'content-type': 'application/xml; charset=utf-8'}
        response.read_stream = read_stream
        yield cast('HttpResponse', response)

    client = AsyncMock(spec=HttpClient)
    client.stream = stream
    return client, attempts


async def open_throttler_held_until_reclaim(
    monkeypatch: pytest.MonkeyPatch,
) -> ThrottlingRequestManager[RequestQueue]:
    """Open a throttler for `throttled.placeholder.com` whose backoff lasts until the crawler gives a request back.

    The throttler runs on a stopped clock, so a backoff can't run out before the crawler checks whether the domain is
    held back. Each reclaimed request moves the clock past the backoff, so a deferred request is dispatched right away.
    """
    backoff = timedelta(minutes=1)
    clock = Mock()
    clock.now.return_value = datetime.now(timezone.utc)
    monkeypatch.setattr(_throttling_request_manager, 'datetime', clock)
    throttler = ThrottlingRequestManager(
        await RequestQueue.open(),
        domains=['throttled.placeholder.com'],
        request_manager_opener=RequestQueue.open,
        base_delay=backoff,
        max_delay=backoff,
    )
    reclaim_request = throttler.reclaim_request

    async def reclaim_after_backoff(request: Request, *, forefront: bool = False) -> ProcessedRequest | None:
        clock.now.return_value += backoff
        return await reclaim_request(request, forefront=forefront)

    monkeypatch.setattr(throttler, 'reclaim_request', reclaim_after_backoff)
    return throttler


async def maybe_await(value: Awaitable[T] | T) -> T:
    """Await `value` if it is awaitable, otherwise return it unchanged.

    Lets `poll_until_condition` accept both sync and async callables.
    """
    if inspect.isawaitable(value):
        return await cast('Awaitable[T]', value)
    return cast('T', value)


@overload
async def poll_until_condition(
    fn: Callable[[], Awaitable[T]],
    condition: Callable[[T], bool] = ...,
    *,
    timeout: float = ...,
    poll_interval: float = ...,
    backoff_factor: float = ...,
) -> T: ...
@overload
async def poll_until_condition(
    fn: Callable[[], T],
    condition: Callable[[T], bool] = ...,
    *,
    timeout: float = ...,
    poll_interval: float = ...,
    backoff_factor: float = ...,
) -> T: ...
async def poll_until_condition(
    fn: Callable[[], Awaitable[T] | T],
    condition: Callable[[T], bool] = bool,
    *,
    timeout: float = 5,
    poll_interval: float = 0.05,
    backoff_factor: float = 1,
) -> T:
    """Poll `fn` until `condition(result)` is True or the timeout expires.

    Polls `fn` at `poll_interval`-second intervals until `condition` is satisfied or `timeout` seconds have elapsed.
    Returns the last polled result regardless of whether the condition was met, so the caller can run its own
    assertion. The default condition checks for a truthy result.

    Use this instead of a fixed `asyncio.sleep` when waiting for some state to settle (e.g. autoscaling
    concurrency) that may take a variable amount of time. For highly variable waits, pass `backoff_factor` > 1
    to multiply the interval after each poll, covering a long timeout with few calls.
    """
    deadline = time.monotonic() + timeout
    delay = poll_interval
    result = await maybe_await(fn())
    while not condition(result):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        await asyncio.sleep(min(delay, remaining))
        delay *= backoff_factor
        result = await maybe_await(fn())
    return result


DEFAULT_URL = 'http://not-exists.com/'


def get_basic_sitemap(url: str | URL = DEFAULT_URL) -> str:
    return """
    <?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url>
    <loc>{url}</loc>
    <lastmod>2005-02-03</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
    </url>
    <url>
    <loc>{url}catalog?item=12&amp;desc=vacation_hawaii</loc>
    <changefreq>weekly</changefreq>
    </url>
    <url>
    <loc>{url}catalog?item=73&amp;desc=vacation_new_zealand</loc>
    <lastmod>2004-12-23</lastmod>
    <changefreq>weekly</changefreq>
    </url>
    <url>
    <loc>{url}catalog?item=74&amp;desc=vacation_newfoundland</loc>
    <lastmod>2004-12-23T18:00:15+00:00</lastmod>
    <priority>0.3</priority>
    </url>
    <url>
    <loc>{url}catalog?item=83&amp;desc=vacation_usa</loc>
    <lastmod>2004-11-23</lastmod>
    </url>
    </urlset>
    """.strip().format(url=url)


def get_basic_results(server_url: str | URL = DEFAULT_URL) -> set[str]:
    return {
        str(server_url),
        f'{server_url}catalog?item=12&desc=vacation_hawaii',
        f'{server_url}catalog?item=73&desc=vacation_new_zealand',
        f'{server_url}catalog?item=74&desc=vacation_newfoundland',
        f'{server_url}catalog?item=83&desc=vacation_usa',
    }
