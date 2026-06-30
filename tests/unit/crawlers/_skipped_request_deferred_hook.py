"""A user's skipped-request hook, written in the idiomatic modern style.

This file deliberately mimics how a real Crawlee user would put a hook in their own module:

* ``from __future__ import annotations`` (PEP 563) turns every annotation into a *string*, so
  ``request: Request`` is stored as the literal ``"Request"`` and only resolved on demand.
* ``Request`` is imported only under ``TYPE_CHECKING`` -- the common pattern recommended to avoid
  paying an import at runtime and to sidestep import cycles. Crawlee's own source uses this style
  throughout.

The file name starts with an underscore and is not ``test_*`` so pytest does not collect it; the
test loads it by path to simulate "the user's hook lives in their own module".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee import Request
    from crawlee._types import SkippedReason

# Records whatever object the crawler actually hands to the callback.
received: list[object] = []


async def on_skipped_request(request: Request, reason: SkippedReason) -> None:
    """The user annotated the first parameter as ``Request`` because they want the full object.

    A realistic body would read ``request.user_data`` / ``request.label`` here -- which is the whole
    reason PR #1999 exists. We only record the received object so the test can assert its type.
    """
    received.append(request)
