from enum import Enum
from typing import Any, Callable, Coroutine, Union


class Event(Enum):
    """Enum of all possible events that can be emitted."""

    PERSIST_STATE = 'persistState'
    SYSTEM_INFO = 'systemInfo'
    MIGRATING = 'migrating'
    ABORTING = 'aborting'
    EXIT = 'exit'


SyncListener = Callable[..., None]
AsyncListener = Callable[..., Coroutine[Any, Any, None]]
Listener = Union[SyncListener, AsyncListener]
WrappedListener = Callable[..., Coroutine[Any, Any, None]]
