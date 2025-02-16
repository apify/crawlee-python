from __future__ import annotations

from collections.abc import Callable, Coroutine
from enum import Enum
from typing import Annotated, Any, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field

from crawlee._utils.docs import docs_group
from crawlee._utils.models import timedelta_secs
from crawlee._utils.system import CpuInfo, MemoryUsageInfo


class Event(str, Enum):
    """Names of all possible events that can be emitted using an `EventManager`."""

    # Core events
    PERSIST_STATE = 'persistState'
    SYSTEM_INFO = 'systemInfo'
    MIGRATING = 'migrating'
    ABORTING = 'aborting'
    EXIT = 'exit'

    # Session pool events
    SESSION_RETIRED = 'sessionRetired'

    # Browser pool events
    BROWSER_LAUNCHED = 'browserLaunched'
    BROWSER_RETIRED = 'browserRetired'
    BROWSER_CLOSED = 'browserClosed'
    PAGE_CREATED = 'pageCreated'
    PAGE_CLOSED = 'pageClosed'


@docs_group('Event payloads')
class EventPersistStateData(BaseModel):
    """Data for the persist state event."""

    model_config = ConfigDict(populate_by_name=True)

    is_migrating: Annotated[bool, Field(alias='isMigrating')]


@docs_group('Event payloads')
class EventSystemInfoData(BaseModel):
    """Data for the system info event."""

    model_config = ConfigDict(populate_by_name=True)

    cpu_info: Annotated[CpuInfo, Field(alias='cpuInfo')]
    memory_info: Annotated[
        MemoryUsageInfo,
        Field(alias='memoryInfo'),
    ]


@docs_group('Event payloads')
class EventMigratingData(BaseModel):
    """Data for the migrating event."""

    model_config = ConfigDict(populate_by_name=True)

    # The remaining time in seconds before the migration is forced and the process is killed
    # Optional because it's not present when the event handler is called manually
    time_remaining: Annotated[timedelta_secs | None, Field(alias='timeRemainingSecs')] = None


@docs_group('Event payloads')
class EventAbortingData(BaseModel):
    """Data for the aborting event."""

    model_config = ConfigDict(populate_by_name=True)


@docs_group('Event payloads')
class EventExitData(BaseModel):
    """Data for the exit event."""

    model_config = ConfigDict(populate_by_name=True)


EventData = Union[EventPersistStateData, EventSystemInfoData, EventMigratingData, EventAbortingData, EventExitData]
"""A helper type for all possible event payloads"""

WrappedListener = Callable[..., Coroutine[Any, Any, None]]

TEvent = TypeVar('TEvent')
EventListener = Union[
    Callable[
        [TEvent],
        Union[None, Coroutine[Any, Any, None]],
    ],
    Callable[
        [],
        Union[None, Coroutine[Any, Any, None]],
    ],
]
"""An event listener function - it can be both sync and async and may accept zero or one argument."""
