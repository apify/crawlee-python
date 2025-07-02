# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session_pool.ts

from __future__ import annotations

import random
from collections.abc import Callable
from logging import getLogger
from typing import TYPE_CHECKING, Literal, overload

from crawlee import service_locator
from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee._utils.recoverable_state import RecoverableState
from crawlee.sessions import Session
from crawlee.sessions._models import SessionPoolModel

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events import EventManager

logger = getLogger(__name__)

CreateSessionFunctionType = Callable[[], Session]


@docs_group('Classes')
class SessionPool:
    """A pool of sessions that are managed, rotated, and persisted based on usage and age.

    It ensures effective session management by maintaining a pool of sessions and rotating them based on
    usage count, expiration time, or custom rules. It provides methods to retrieve sessions, manage their
    lifecycle, and optionally persist the state to enable recovery.
    """

    def __init__(
        self,
        *,
        max_pool_size: int = 1000,
        create_session_settings: dict | None = None,
        create_session_function: CreateSessionFunctionType | None = None,
        event_manager: EventManager | None = None,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str | None = None,
        persist_state_key: str = 'CRAWLEE_SESSION_POOL_STATE',
    ) -> None:
        """Initialize a new instance.

        Args:
            max_pool_size: Maximum number of sessions to maintain in the pool. You can add more sessions to the pool
                by using the `add_session` method.
            create_session_settings: Settings for creating new session instances. If None, default settings will
                be used. Do not set it if you are providing a `create_session_function`.
            create_session_function: A callable to create new session instances. If None, a default session settings
                will be used. Do not set it if you are providing `create_session_settings`.
            event_manager: The event manager to handle events like persist state.
            persistence_enabled: Flag to enable or disable state persistence of the pool.
            persist_state_kvs_name: The name of the `KeyValueStore` used for state persistence.
            persist_state_key: The key under which the session pool's state is stored in the `KeyValueStore`.
        """
        if event_manager:
            service_locator.set_event_manager(event_manager)

        self._state = RecoverableState(
            default_state=SessionPoolModel(
                max_pool_size=max_pool_size,
                sessions={},
            ),
            logger=logger,
            persistence_enabled=persistence_enabled,
            persist_state_kvs_name=persist_state_kvs_name,
            persist_state_key=persist_state_key or 'CRAWLEE_SESSION_POOL_STATE',
        )

        self._max_pool_size = max_pool_size
        self._session_settings = create_session_settings or {}
        self._create_session_function = create_session_function
        self._persistence_enabled = persistence_enabled

        if self._create_session_function and self._session_settings:
            raise ValueError('Both `create_session_settings` and `create_session_function` cannot be provided.')

        # Flag to indicate the context state.
        self._active = False

    def __repr__(self) -> str:
        """Get a string representation."""
        return f'<{self.__class__.__name__} {self.get_state(as_dict=False)}>'

    @property
    def session_count(self) -> int:
        """Get the total number of sessions currently maintained in the pool."""
        return len(self._state.current_value.sessions)

    @property
    def usable_session_count(self) -> int:
        """Get the number of sessions that are currently usable."""
        return self._state.current_value.usable_session_count

    @property
    def retired_session_count(self) -> int:
        """Get the number of sessions that are no longer usable."""
        return self._state.current_value.retired_session_count

    @property
    def active(self) -> bool:
        """Indicate whether the context is active."""
        return self._active

    async def __aenter__(self) -> SessionPool:
        """Initialize the pool upon entering the context manager.

        Raises:
            RuntimeError: If the context manager is already active.
        """
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')

        self._active = True

        state = await self._state.initialize()
        state.max_pool_size = self._max_pool_size
        self._remove_retired_sessions()

        if not state.sessions:
            await self._fill_sessions_to_max()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Deinitialize the pool upon exiting the context manager.

        Raises:
            RuntimeError: If the context manager is not active.
        """
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')

        await self._state.teardown()

        self._active = False

    @overload
    def get_state(self, *, as_dict: Literal[True]) -> dict: ...

    @overload
    def get_state(self, *, as_dict: Literal[False]) -> SessionPoolModel: ...

    @ensure_context
    def get_state(self, *, as_dict: bool = False) -> SessionPoolModel | dict:
        """Retrieve the current state of the pool either as a model or as a dictionary."""
        model = self._state.current_value.model_copy(deep=True)
        if as_dict:
            return model.model_dump()
        return model

    @ensure_context
    def add_session(self, session: Session) -> None:
        """Add an externally created session to the pool.

        This is intened only for the cases when you want to add a session that was created outside of the pool.
        Otherwise, the pool will create new sessions automatically.

        Args:
            session: The session to add to the pool.
        """
        state = self._state.current_value

        if session.id in state.sessions:
            logger.warning(f'Session with ID {session.id} already exists in the pool.')
            return
        state.sessions[session.id] = session

    @ensure_context
    async def get_session(self) -> Session:
        """Retrieve a random session from the pool.

        This method first ensures the session pool is at its maximum capacity. If the random session is not usable,
        retired sessions are removed and a new session is created and returned.

        Returns:
            The session object.
        """
        await self._fill_sessions_to_max()
        session = self._get_random_session()

        if session.is_usable:
            return session

        # If the random session is not usable, clean up and create a new session
        self._remove_retired_sessions()
        return await self._create_new_session()

    @ensure_context
    async def get_session_by_id(self, session_id: str) -> Session | None:
        """Retrieve a session by ID from the pool.

        This method first ensures the session pool is at its maximum capacity. It then tries to retrieve a specific
        session by ID. If the session is not found or not usable, `None` is returned.

        Args:
            session_id: The ID of the session to retrieve.

        Returns:
            The session object if found and usable, otherwise `None`.
        """
        await self._fill_sessions_to_max()
        session = self._state.current_value.sessions.get(session_id)

        if not session:
            logger.warning(f'Session with ID {session_id} not found.')
            return None

        if not session.is_usable:
            logger.warning(f'Session with ID {session_id} is not usable.')
            return None

        return session

    async def reset_store(self) -> None:
        """Reset the KVS where the pool state is persisted."""
        await self._state.reset()

    async def _create_new_session(self) -> Session:
        """Create a new session, add it to the pool and return it."""
        if self._create_session_function:
            new_session = self._create_session_function()
        else:
            new_session = Session(**self._session_settings)
        self._state.current_value.sessions[new_session.id] = new_session
        return new_session

    async def _fill_sessions_to_max(self) -> None:
        """Fill the pool with sessions to the maximum size."""
        for _ in range(self._max_pool_size - self.session_count):
            await self._create_new_session()

    def _get_random_session(self) -> Session:
        """Get a random session from the pool."""
        state = self._state.current_value
        if not state.sessions:
            raise ValueError('No sessions available in the pool.')
        return random.choice(list(state.sessions.values()))

    def _remove_retired_sessions(self) -> None:
        """Remove all sessions from the pool that are no longer usable."""
        state = self._state.current_value
        state.sessions = {session.id: session for session in state.sessions.values() if session.is_usable}
