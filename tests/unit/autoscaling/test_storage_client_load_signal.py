from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import Mock

import pytest

from crawlee import service_locator
from crawlee.autoscaling import LoadSignalStartContext, StorageClientLoadSignal, StorageClientLoadSignalOptions
from crawlee.configuration import Configuration
from crawlee.storage_clients import StorageClient


async def test_overloaded_when_errors_grow(monkeypatch: pytest.MonkeyPatch) -> None:
    """The storage client counts as overloaded once its errors grow by more than `Configuration.max_client_errors`."""
    storage_client = Mock(spec=StorageClient)
    # The errors seen on the second retry are the ones that count.
    storage_client.get_rate_limit_errors.side_effect = [{2: 10}, {1: 50, 2: 16}, {1: 100, 2: 21}]
    monkeypatch.setattr(service_locator, 'get_storage_client', Mock(return_value=storage_client))

    signal = StorageClientLoadSignal(StorageClientLoadSignalOptions(snapshot_interval=timedelta(hours=1)))
    # Not the default, so the signal must read it from the configuration.
    configuration = Configuration(max_client_errors=5)
    await signal.start(LoadSignalStartContext(max_sample_window=timedelta(seconds=30), configuration=configuration))

    # The first snapshot is taken right after the start, and the errors seen before it are not new.
    await asyncio.sleep(0)

    for _ in range(2):
        signal._snapshot()

    await signal.stop()

    assert [snapshot.is_overloaded for snapshot in signal.get_sample()] == [False, True, False]


async def test_restart_ignores_errors_of_previous_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """A restarted signal does not count the errors seen before the restart as new ones."""
    storage_client = Mock(spec=StorageClient)
    storage_client.get_rate_limit_errors.side_effect = [{2: 0}, {2: 10}]
    monkeypatch.setattr(service_locator, 'get_storage_client', Mock(return_value=storage_client))

    signal = StorageClientLoadSignal()
    context = LoadSignalStartContext(
        max_sample_window=timedelta(seconds=30),
        configuration=Configuration(max_client_errors=3),
    )

    await signal.start(context)
    signal._snapshot()
    await signal.stop()

    await signal.start(context)
    signal._snapshot()
    await signal.stop()

    assert [snapshot.is_overloaded for snapshot in signal.get_sample()] == [False]


@pytest.mark.parametrize(
    'snapshot_interval',
    [
        pytest.param(timedelta(0), id='zero'),
        pytest.param(timedelta(seconds=-1), id='negative'),
    ],
)
def test_rejects_non_positive_interval(snapshot_interval: timedelta) -> None:
    """A non-positive snapshot interval is refused, as the measuring task would spin on the event loop."""
    with pytest.raises(ValueError, match='snapshot_interval must be positive'):
        StorageClientLoadSignalOptions(snapshot_interval=snapshot_interval)
