from __future__ import annotations

from typing import TYPE_CHECKING

from ._types import ReadyStatus, StalledStatus, WaitingStatus

if TYPE_CHECKING:
    from ._types import RequestSourceStatus


def join_request_source_statuses(
    first_status: RequestSourceStatus,
    second_status: RequestSourceStatus,
) -> RequestSourceStatus:
    """Combine two statuses with the precedence ready > waiting > stalled > finished.

    Of two waiting statuses, the one with the earlier known `ready_at` wins.
    """
    if isinstance(first_status, ReadyStatus):
        return first_status
    if isinstance(second_status, ReadyStatus):
        return second_status

    if isinstance(first_status, WaitingStatus):
        if not isinstance(second_status, WaitingStatus) or second_status.ready_at is None:
            return first_status
        if first_status.ready_at is not None and first_status.ready_at <= second_status.ready_at:
            return first_status
        return second_status

    if isinstance(second_status, WaitingStatus):
        return second_status

    if isinstance(first_status, StalledStatus):
        return first_status

    # `first_status` is finished, so `second_status` decides.
    return second_status
