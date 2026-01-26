from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from crawlee._request import Request

    from ._basic_crawling_context import BasicCrawlingContext


@contextmanager
def swapped_context(
    context: BasicCrawlingContext,
    request: Request,
) -> Iterator[None]:
    """Replace context's isolated copies with originals after handler execution."""
    try:
        yield
    finally:
        # Restore original context state to avoid side effects between different handlers.
        object.__setattr__(context, 'request', request)
