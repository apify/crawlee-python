"""Skipped-request hooks written in the idiomatic deferred-annotation style.

Used by `test_basic_crawler.py` to check that `_skipped_request_callback_expects_request` still
recognizes a `Request` annotation when the hook's module uses `from __future__ import annotations`
(PEP 563) and imports `Request` only under `TYPE_CHECKING`, so the name is not available at runtime.
The module is loaded by file path so it is never collected by pytest and does not rely on the test
package layout.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee import Request, SkippedReason


async def expects_request(_request: Request, _reason: SkippedReason) -> None:
    """First parameter annotated `Request`, resolvable only under `TYPE_CHECKING`."""


async def expects_optional_request(_request: Request | None, _reason: SkippedReason) -> None:
    """First parameter annotated with a `Request | None` union."""


async def expects_url(_url: str, _reason: SkippedReason) -> None:
    """First parameter annotated `str` keeps the legacy URL-only behavior."""
