# ruff: noqa: TCH001 TCH002
from __future__ import annotations

from collections.abc import Callable, Coroutine
from enum import Enum
from typing import Annotated, Any, Union

from pydantic import BaseModel, ConfigDict, Field

from crawlee._utils.system import CpuInfo, MemoryInfo


class Event(str, Enum):
    """Enum of all possible events that can be emitted."""

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


class EventPersistStateData(BaseModel):
    """Data for the persist state event."""

    model_config = ConfigDict(populate_by_name=True)

    is_migrating: Annotated[bool, Field(alias='isMigrating')]


class EventSystemInfoData(BaseModel):
    """Data for the system info event."""

    model_config = ConfigDict(populate_by_name=True)

    cpu_info: Annotated[CpuInfo, Field(alias='cpuInfo')]
    memory_info: Annotated[MemoryInfo, Field(alias='memoryInfo')]


class EventMigratingData(BaseModel):
    """Data for the migrating event."""

    model_config = ConfigDict(populate_by_name=True)


class EventAbortingData(BaseModel):
    """Data for the aborting event."""

    model_config = ConfigDict(populate_by_name=True)


class EventExitData(BaseModel):
    """Data for the exit event."""

    model_config = ConfigDict(populate_by_name=True)


EventData = Union[EventPersistStateData, EventSystemInfoData, EventMigratingData, EventAbortingData, EventExitData]
SyncListener = Callable[..., None]
AsyncListener = Callable[..., Coroutine[Any, Any, None]]
Listener = Union[SyncListener, AsyncListener]
WrappedListener = Callable[..., Coroutine[Any, Any, None]]
