from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from crawlee._utils.system import CpuInfo, MemoryInfo


class Event(Enum):
    """Enum of all possible events that can be emitted."""

    PERSIST_STATE = 'persistState'
    SYSTEM_INFO = 'systemInfo'
    MIGRATING = 'migrating'
    ABORTING = 'aborting'
    EXIT = 'exit'
    SESSION_RETIRED = 'sessionRetired'


@dataclass
class EventPersistStateData:
    """Data for the persist state event."""

    is_migrating: bool


@dataclass
class EventSystemInfoData:
    """Data for the system info event."""

    cpu_info: CpuInfo
    memory_info: MemoryInfo


@dataclass
class EventMigratingData:
    """Data for the migrating event."""


@dataclass
class EventAbortingData:
    """Data for the aborting event."""


@dataclass
class EventExitData:
    """Data for the exit event."""


EventData = Union[EventPersistStateData, EventSystemInfoData, EventMigratingData, EventAbortingData, EventExitData]
SyncListener = Callable[..., None]
AsyncListener = Callable[..., Coroutine[Any, Any, None]]
Listener = Union[SyncListener, AsyncListener]
WrappedListener = Callable[..., Coroutine[Any, Any, None]]
