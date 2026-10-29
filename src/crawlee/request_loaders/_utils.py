from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._types import RequestSourceStatus


def join_request_source_statuses(
    first: RequestSourceStatus,
    second: RequestSourceStatus,
) -> RequestSourceStatus:
    """Combine two statuses with the precedence ready > waiting > stalled > finished.

    Of two waiting statuses, the one with the earlier known `ready_at` wins.
    """
    if first.status == 'ready':
        return first
    if second.status == 'ready':
        return second

    if first.status == 'waiting':
        # A known `ready_at` lets a consumer schedule a wake-up on top of regular polling, so it beats an unknown one.
        if second.status != 'waiting' or second.ready_at is None:
            return first
        if first.ready_at is not None and first.ready_at <= second.ready_at:
            return first
        return second

    if second.status == 'waiting':
        return second

    if first.status == 'stalled':
        return first

    # `first` is finished, so `second` decides.
    return second
