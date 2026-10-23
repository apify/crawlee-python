from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock, patch

import pytest

from crawlee.autoscaling import ConcurrencySystem
from crawlee.autoscaling._snapshotter import Snapshotter
from crawlee.autoscaling._system_status import SystemStatus

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def system_status() -> Mock:
    """The system load verdicts the test hands to the concurrency system. Unconfigured, it reports an idle system."""
    return Mock(spec=SystemStatus)


@pytest.fixture
def make_concurrency_system(system_status: Mock) -> Iterator[type[ConcurrencySystem]]:
    """A `ConcurrencySystem` factory with the load measurement replaced by `system_status`."""
    with (
        patch.object(Snapshotter, 'from_config', return_value=AsyncMock(spec=Snapshotter)),
        patch('crawlee.autoscaling._concurrency_system.SystemStatus', return_value=system_status),
    ):
        yield ConcurrencySystem
