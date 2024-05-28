from __future__ import annotations

from enum import Enum


class EnqueueStrategy(str, Enum):
    """Strategy for deciding which links should be followed and which ones should be ignored."""

    ALL = 'all'
    SAME_DOMAIN = 'same-domain'
    SAME_HOSTNAME = 'same-hostname'
    SAME_ORIGIN = 'same-origin'
