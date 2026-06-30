"""Failing tests accompanying the internal review of PR #1999.

PR #1999 lets ``on_skipped_request`` callbacks receive the full ``Request`` object instead of only
the URL string. It decides which form to pass by *introspecting the type annotation* of the
callback's first parameter at registration time (``_skipped_request_callback_expects_request``), and
it also routes robots.txt-skipped links through ``transform_request_function``.

Each test below encodes a concrete, realistic user scenario that the current implementation handles
incorrectly. They are expected to FAIL against the PR as written; they document the behavior a
correct implementation should have.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

from crawlee import Request, SkippedReason
from crawlee._request import RequestOptions
from crawlee._types import RequestTransformAction
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.crawlers._basic._basic_crawler import (
    BasicCrawler,
    _skipped_request_callback_expects_request,
)

if TYPE_CHECKING:
    from yarl import URL


# ---------------------------------------------------------------------------------------------------
# Finding #1 -- annotation-driven dispatch fails silently for idiomatic, runtime-deferred annotations.
# ---------------------------------------------------------------------------------------------------


async def test_skipped_callback_with_request_annotation_deferred_under_type_checking(server_url: URL) -> None:
    """A hook annotated ``Request`` in a module using ``from __future__ import annotations`` + TYPE_CHECKING.

    Real use case
    -------------
    A user keeps their crawler hooks in their own module, written in the modern idiomatic style that
    Crawlee itself recommends and uses everywhere::

        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from crawlee import Request, SkippedReason

        async def on_skipped_request(request: Request, reason: SkippedReason) -> None:
            log_blocked(request.url, request.user_data)   # they want the Request object

    They register it and expect to receive ``Request`` objects, exactly as the PR's docs example
    promises. But ``_skipped_request_callback_expects_request`` calls ``get_type_hints`` on the
    callback, and because ``Request`` is only a string annotation resolved against the hook module's
    globals -- where ``Request`` is *not* present at runtime (TYPE_CHECKING-only import) -- the lookup
    raises ``NameError``. The blanket ``except Exception: return False`` then silently downgrades the
    user to the legacy ``str`` form, with no warning. The user gets a ``str`` and any
    ``request.user_data`` access blows up at runtime.
    """
    # Load the user's hook module exactly as it would exist in their project (by file path, so the
    # test does not depend on package layout / sys.path).
    hook_path = Path(__file__).parent / '_skipped_request_deferred_hook.py'
    spec = importlib.util.spec_from_file_location('user_project_hooks', hook_path)
    assert spec is not None and spec.loader is not None
    hook_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(hook_module)
    hook_module.received.clear()

    crawler = BasicCrawler(respect_robots_txt_file=True)
    crawler.on_skipped_request(hook_module.on_skipped_request)

    # `page_1` is disallowed by the test server's robots.txt (`Disallow: /page_`).
    await crawler.add_requests([str(server_url / 'page_1')])

    assert hook_module.received, 'the skipped-request callback was never invoked'
    # The user asked for a `Request` (annotated as such); they must not silently receive a bare URL.
    assert all(isinstance(received, Request) for received in hook_module.received), (
        f'callback received {type(hook_module.received[0]).__name__}, expected Request -- '
        'the Request annotation was silently ignored'
    )


def test_skipped_request_callback_detection_accepts_optional_request_annotation() -> None:
    """Annotating the parameter ``Request | None`` should still be recognized as "wants the Request".

    Real use case
    -------------
    A user writes a single reusable hook and annotates its parameter ``Request | None`` -- a perfectly
    normal, explicit annotation (and the exact shape Crawlee already uses for ``ErrorHandler``, which
    returns ``Request | None``). They clearly want the ``Request``. But detection uses exact identity
    (``type_hints.get(name) is Request``), so the union does not match and the user is silently
    downgraded to the ``str`` form.
    """

    async def hook(request: Request | None, reason: SkippedReason) -> None: ...

    assert _skipped_request_callback_expects_request(hook) is True


# ---------------------------------------------------------------------------------------------------
# Finding #2 -- transform_request_function is now applied to robots.txt-skipped links, which can drop
#              URLs from the skipped-request callback entirely.
# ---------------------------------------------------------------------------------------------------


async def test_robots_skipped_callback_not_suppressed_by_transform_skip(server_url: URL) -> None:
    """A ``transform_request_function`` returning ``'skip'`` must not hide URLs from the robots audit.

    Real use case
    -------------
    A user respects robots.txt and, separately, uses ``enqueue_links(transform_request_function=...)``
    to drop some URLs from the crawl (returning ``'skip'``). They also register ``on_skipped_request``
    to audit *every* URL that robots.txt blocked -- e.g. to report crawl coverage or flag
    misconfigured rules.

    Before PR #1999, robots-skipped URLs were reported to the callback directly. The PR now routes
    them through ``transform_request_function`` first, so a URL that is *both* robots-disallowed and
    matched by the user's skip rule (here ``page_3``) silently disappears from the robots audit -- the
    callback is never called for it, even though robots.txt is the reason it cannot be crawled.

    The test server disallows ``/page_*`` and the start page links to page_1..page_4, so all four are
    robots-skipped. The transform skips ``page_3``; the robots audit should still see all four.
    """
    crawler = BeautifulSoupCrawler(respect_robots_txt_file=True)
    skip = mock.Mock()

    def transform(options: RequestOptions) -> RequestOptions | RequestTransformAction:
        # The user only means "do not enqueue page_3"; they are not touching robots handling.
        if 'page_3' in options['url']:
            return 'skip'
        return 'unchanged'

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        await context.enqueue_links(transform_request_function=transform)

    @crawler.on_skipped_request
    async def skipped_hook(request: Request, _reason: SkippedReason) -> None:
        skip(request.url)

    await crawler.run([str(server_url / 'start_enqueue')])

    reported = {call.args[0] for call in skip.call_args_list}
    expected = {
        str(server_url / 'page_1'),
        str(server_url / 'page_2'),
        str(server_url / 'page_3'),  # robots-blocked; skipping it from enqueue must not hide it here
        str(server_url / 'page_4'),
    }
    assert reported == expected, f'robots-skipped URLs missing from callback: {expected - reported}'
