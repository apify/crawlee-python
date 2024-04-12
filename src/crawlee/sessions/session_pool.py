# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session_pool.ts

from __future__ import annotations

import random
from logging import getLogger
from typing import TYPE_CHECKING

from crawlee.events.types import Event, EventPersistStateData
from crawlee.sessions.session import Session
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events import EventManager

logger = getLogger(__name__)


# TODO:
# - max_listeners (default 20) ?
# - implement reset_store method?
# - create_session_function: Callable | None = None,


class SessionPool:
    """Session pool is a pool of sessions that are rotated based on the usage count or age."""

    def __init__(
        self,
        event_manager: EventManager,
        *,
        max_pool_size: int = 1000,
        persist_state_key_value_store_name: str = 'default',
        persist_state_key: str = 'CRAWLEE_SESSION_POOL_STATE',
        persistence_options: dict | None = None,
        session_settings: dict | None = None,
    ) -> None:
        """Create a new instance.

        Args:
            event_manager: Event manager instance.
            max_pool_size: Maximum size of the pool. Indicates how many sessions are rotated.
            persist_state_key_value_store_name: Name of `KeyValueStore` where is the `SessionPool` state stored.
            persist_state_key: Session pool persists it's state under this key in Key value store.
            persistence_options: Control how and when to persist the state of the session pool.
            session_settings: Settings for creation of the sessions.
            create_session_function: Custom function that should return `Session` instance.
        """
        self._event_manager = event_manager
        self._max_pool_size = max_pool_size
        self._persist_state_kvs_name = persist_state_key_value_store_name
        self._persist_state_key = persist_state_key
        self._persistence_options = persistence_options or {}
        self._session_settings = session_settings or {}

        self._kvs: KeyValueStore | None = None
        self._sessions: dict[str, Session] = {}

    @property
    def session_count(self) -> int:
        """Return the number of sessions."""
        return len(self._sessions)

    @property
    def usable_session_count(self) -> int:
        """Return the number of usable sessions."""
        return len([session for _, session in self._sessions.items() if session.is_usable])

    @property
    def retired_session_count(self) -> int:
        """Return the number of retired sessions."""
        return len([session for _, session in self._sessions.items() if not session.is_usable])

    def get_state(self) -> dict:
        """Return the state of the session pool."""
        return {
            'usable_session_count': self.usable_session_count,
            'retired_session_count': self.retired_session_count,
            'sessions': [session.get_state() for _, session in self._sessions.items()],
        }

    async def __aenter__(self) -> SessionPool:
        """Initialize the session pool."""
        self._kvs = await KeyValueStore.open(name=self._persist_state_kvs_name)

        # Try to restore the previous state of the session pool
        was_restored = await self._try_to_restore_previos_state()

        # If the pool was not restored, create new sessions
        if not was_restored:
            for _ in range(self._max_pool_size):
                await self._create_new_session()

        # Register event listener for persisting state
        self._event_manager.on(event=Event.PERSIST_STATE, listener=self._persist_state)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Deinitialize the session pool."""
        self._event_manager.off(event=Event.PERSIST_STATE, listener=self._persist_state)
        await self._persist_state(event_data=EventPersistStateData(is_migrating=False))

    async def get_session(self, *, session_id: str | None = None) -> Session | None:
        """Get a session from the pool."""
        await self._fill_sessions_to_max()

        # If session_id is provided, return the session with the given ID
        if session_id:
            session = self._sessions.get(session_id)

            if not session:
                logger.warning(f'Session with ID {session_id} not found.')
                return None

            if not session.is_usable:
                logger.warning(f'Session with ID {session_id} is not usable.')
                return None

            return session

        session = self._get_random_session()

        if session.is_usable:
            return session

        self._remove_retired_sessions()
        return await self._create_new_session()

    async def _create_new_session(self) -> Session:
        """Create a new session."""
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
        """Removes all sessions from the pool that are no longer usable."""
        self._sessions = {session_id: session for session_id, session in self._sessions.items() if session.is_usable}

    async def _try_to_restore_previos_state(self) -> bool:
        """Try to restore the previous state of the pool from the KVS."""
        if not self._kvs:
            logger.warning('SessionPool restoration failed: KVS not initialized. Did you forget to call __aenter__?')
            return False

        previous_state: dict | None = await self._kvs.get_value(key=self._persist_state_key)

        if previous_state:
            for session_state in previous_state['sessions']:
                session = Session.from_kwargs(**session_state)
                if session.is_usable:
                    self._sessions[session.id] = session
            return True

        return False

    async def _persist_state(self, event_data: EventPersistStateData) -> None:
        """Persist the state of the pool in the KVS."""
        logger.debug(f'Persisting state of the SessionPool (event_data={event_data}).')

        if not self._kvs:
            logger.warning('SessionPool persisting failed: KVS not initialized. Did you forget to call __aenter__?')
            return

        session_pool_state = self.get_state()
        await self._kvs.set_value(key=self._persist_state_key, value=session_pool_state)
