# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session_pool.ts

# TODO:
# - max_listeners (default 20) ?
# - create_session_function: Callable | None = None,

from __future__ import annotations

import random
from logging import getLogger
from typing import TYPE_CHECKING, Callable, Literal, overload

from crawlee.events.types import Event, EventPersistStateData
from crawlee.sessions import Session
from crawlee.sessions.models import SessionPoolModel
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events import EventManager

logger = getLogger(__name__)


class SessionPool:
    """Session pool is a pool of sessions that are rotated based on the usage count or age."""

    def __init__(
        self,
        *,
        max_pool_size: int = 1000,
        create_session_settings: dict | None = None,
        create_session_function: Callable | None = None,
        event_manager: EventManager | None = None,
        persistance_enabled: bool = False,
        persist_state_kvs_name: str = 'default',
        persist_state_key: str = 'CRAWLEE_SESSION_POOL_STATE',
    ) -> None:
        """Create a new instance.

        Args:
            max_pool_size: Maximum number of sessions to maintain in the pool.

            create_session_settings: Settings for creating new session instances.
                If None, default settings will be used.

            create_session_function: A callable to create new session instances.
                If None, a default session settings will be used.

            event_manager: The event manager to handle events like persist state.

            persistance_enabled: Flag to enable or disable state persistence of the pool.

            persist_state_kvs_name: The name of the `KeyValueStore` used for state persistence.

            persist_state_key: The key under which the session pool's state is stored in the `KeyValueStore`.
        """
        self._max_pool_size = max_pool_size
        self._session_settings = create_session_settings or {}
        self._create_session_function = create_session_function
        self._event_manager = event_manager
        self._persistance_enabled = persistance_enabled
        self._persist_state_kvs_name = persist_state_kvs_name
        self._persist_state_key = persist_state_key

        if not self._event_manager:
            logger.warning('Event manager not provided: persistence will not work, turning it off.')

        # Internal non-configurable attributes
        self._kvs: KeyValueStore | None = None
        self._sessions: dict[str, Session] = {}

    def __repr__(self) -> str:
        """Get a string representation."""
        return f'<{self.__class__.__name__} {self.get_state(as_dict=False)}>'

    @property
    def session_count(self) -> int:
        """Get the total number of sessions currently maintained in the pool."""
        return len(self._sessions)

    @property
    def usable_session_count(self) -> int:
        """Get the number of sessions that are currently usable."""
        return len([session for _, session in self._sessions.items() if session.is_usable])

    @property
    def retired_session_count(self) -> int:
        """Get the number of sessions that are no longer usable."""
        return self.session_count - self.usable_session_count

    @overload
    def get_state(self, *, as_dict: Literal[True]) -> dict: ...

    @overload
    def get_state(self, *, as_dict: Literal[False]) -> SessionPoolModel: ...

    def get_state(self, *, as_dict: bool = False) -> SessionPoolModel | dict:
        """Retrieve the current state of the pool either as a model or as a dictionary."""
        model = SessionPoolModel(
            persistance_enabled=self._persistance_enabled,
            persist_state_kvs_name=self._persist_state_kvs_name,
            persist_state_key=self._persist_state_key,
            max_pool_size=self._max_pool_size,
            session_count=self.session_count,
            usable_session_count=self.usable_session_count,
            retired_session_count=self.retired_session_count,
            sessions=[session.get_state(as_dict=False) for _, session in self._sessions.items()],
        )
        if as_dict:
            return model.model_dump()
        return model

    async def __aenter__(self) -> SessionPool:
        """Initialize the pool upon entering the context manager."""
        if self._persistance_enabled and self._event_manager:
            self._kvs = await KeyValueStore.open(name=self._persist_state_kvs_name)

            # Attempt to restore the previously persisted state.
            was_restored = await self._try_to_restore_previos_state()

            # If the pool could not be restored, initialize it with new sessions.
            if not was_restored:
                await self._fill_sessions_to_max()

            # Register an event listener for persisting the session pool state.
            self._event_manager.on(event=Event.PERSIST_STATE, listener=self._persist_state)
        # If persistence is disabled, just fill the pool with sessions.
        else:
            await self._fill_sessions_to_max()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Deinitialize the pool upon exiting the context manager."""
        if self._persistance_enabled and self._event_manager:
            # Remove the event listener for state persistence.
            self._event_manager.off(event=Event.PERSIST_STATE, listener=self._persist_state)

            # Persist the final state of the session pool.
            await self._persist_state(event_data=EventPersistStateData(is_migrating=False))

    async def get_session(self, *, session_id: str | None = None) -> Session | None:
        """Retrieve a session from the pool either by a specific ID or randomly.

        This method first ensures the session pool is at its maximum capacity. It then tries to retrieve a specific
        session by ID if provided. Otherwise, a random session is returned. If the random session is not usable,
        retired sessions are removed and a new session is created and returned.

        Args:
            session_id: The ID of the session to retrieve.

        Returns:
            The session object if found and usable, otherwise `None`.
        """
        await self._fill_sessions_to_max()

        # If an session ID was provided, try to get the session by ID
        if session_id:
            session = self._sessions.get(session_id)

            if not session:
                logger.warning(f'Session with ID {session_id} not found.')
                return None

            if not session.is_usable:
                logger.warning(f'Session with ID {session_id} is not usable.')
                return None

            return session

        # If no ID was provided, get a random session
        session = self._get_random_session()

        if session.is_usable:
            return session

        # If the random session is not usable, clean up and create a new session
        self._remove_retired_sessions()
        return await self._create_new_session()

    async def reset_store(self) -> None:
        """Reset the KVS where the pool state is persisted."""
        if not self._persistance_enabled:
            logger.debug('Persistence is disabled; skipping the reset of the store.')
            return

        if not self._kvs:
            logger.warning('SessionPool reset failed: KVS not initialized. Did you forget to call __aenter__?')
            return

        await self._kvs.set_value(key=self._persist_state_key, value=None)

    async def _create_new_session(self) -> Session:
        """Create a new session, add it to the pool and return it."""
        new_session = Session(**self._session_settings)
        self._sessions[new_session.id] = new_session
        return new_session

    async def _fill_sessions_to_max(self) -> None:
        """Fill the pool with sessions to the maximum size."""
        for _ in range(self._max_pool_size - self.session_count):
            await self._create_new_session()

    def _get_random_session(self) -> Session:
        """Get a random session from the pool."""
        keys = list(self._sessions.keys())
        if not keys:
            raise ValueError('No sessions available in the pool.')
        key = random.choice(keys)
        return self._sessions[key]

    def _remove_retired_sessions(self) -> None:
        """Remove all sessions from the pool that are no longer usable."""
        self._sessions = {session_id: session for session_id, session in self._sessions.items() if session.is_usable}

    async def _try_to_restore_previos_state(self) -> bool:
        """Try to restore the previous state of the pool from the KVS."""
        if not self._persistance_enabled:
            logger.warning('Persistence is disabled, however, the state restoration was triggered.')

        if not self._kvs:
            logger.warning('SessionPool restoration failed: KVS not initialized. Did you forget to call __aenter__?')
            return False

        previous_state = await self._kvs.get_value(key=self._persist_state_key)

        if previous_state is None:
            logger.debug('SessionPool restoration skipped: No previous state found.')
            return False

        if not isinstance(previous_state, dict):
            logger.warning('SessionPool restoration failed: Invalid previous state format.')
            return False

        previous_session_pool = SessionPoolModel(**previous_state)

        for session_model in previous_session_pool.sessions:
            session = Session.from_model(model=session_model)
            if session.is_usable:
                self._sessions[session.id] = session

        return True

    async def _persist_state(self, event_data: EventPersistStateData) -> None:
        """Persist the state of the pool in the KVS."""
        logger.debug(f'Persisting state of the SessionPool (event_data={event_data}).')

        if not self._persistance_enabled:
            logger.warning('Persistence is disabled, however, the state persistence event was triggered.')

        if not self._kvs:
            logger.warning('SessionPool persisting failed: KVS not initialized. Did you forget to call __aenter__?')
            return

        session_pool_state = self.get_state(as_dict=True)
        await self._kvs.set_value(key=self._persist_state_key, value=session_pool_state)
