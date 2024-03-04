from __future__ import annotations

import pytest

from crawlee.autoscaling import Snapshotter, SystemStatus
from crawlee.events import LocalEventManager


@pytest.mark.asyncio()
async def test_start_stop() -> None:
    async with LocalEventManager() as event_manager:
        snapshotter = Snapshotter(event_manager)
        await snapshotter.start()

        system_status = SystemStatus(snapshotter)
        system_status.get_current_status()
        system_status.get_historical_status()

        await snapshotter.stop()
