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
