# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session_pool.ts

from __future__ import annotations

import random
from logging import getLogger
from typing import TYPE_CHECKING, ClassVar

from crawlee.events.types import Event, EventPersistStateData
from crawlee.sessions.session import Session, SessionSettings
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events import EventManager

logger = getLogger(__name__)


# TODO:
# - max_listeners (default 20) ?
# - implement reset_store method?


class SessionPool:
    """Session pool is a pool of sessions that are rotated based on the usage count or age."""

    _DEFAULT_BLOCKED_STATUS_CODES: ClassVar = [401, 403, 429]

    def __init__(
        self,
        event_manager: EventManager,
        *,
        max_pool_size: int = 1000,
        persist_state_key_value_store_id: str = 'default',
        persist_state_key: str = 'CRAWLEE_SESSION_POOL_STATE',
        blocked_status_codes: list[int] | None = None,
        persistence_options: dict | None = None,
        session_settings: SessionSettings | None = None,
    ) -> None:
        """Create a new instance.

        Args:
            event_manager: Event manager instance.
            max_pool_size: Maximum size of the pool. Indicates how many sessions are rotated.
            persist_state_key_value_store_id: ID of `KeyValueStore` where is the `SessionPool` state stored.
            persist_state_key: Session pool persists it's state under this key in Key value store.
            blocked_status_codes: Specifies which response status codes are considered as blocked.
            persistence_options: Control how and when to persist the state of the session pool.
            session_settings: Settings for creation of the sessions.
            create_session_function: Custom function that should return `Session` instance.
        """
        self._event_manager = event_manager
        self._max_pool_size = max_pool_size
        self._persist_state_kvs_id = persist_state_key_value_store_id
        self._persist_state_key = persist_state_key
        self._blocked_status_codes = blocked_status_codes or self._DEFAULT_BLOCKED_STATUS_CODES
        self._persistence_options = persistence_options or {}
        self._session_settings = session_settings or SessionSettings()

        self._kvs: KeyValueStore | None = None
        self._sessions: dict[str, Session] = {}

    @property
    def usable_session_count(self) -> int:
        """Return the number of usable sessions."""
        return len([session for _, session in self._sessions.items() if session.is_usable])

    @property
    def retired_session_count(self) -> int:
        """Return the number of retired sessions."""
        return len([session for _, session in self._sessions.items() if not session.is_usable])

    @property
    def state(self) -> dict:
        """Return the state of the session pool."""
        return {
            'usable_session_count': self.usable_session_count,
            'retired_session_count': self.retired_session_count,
            'sessions': {session_id: session.state for session_id, session in self._sessions.items()},
        }

    async def __aenter__(self) -> SessionPool:
        """Initialize the session pool."""
        # TODO: KVS id or name?
        self._kvs = await KeyValueStore.open(name=self._persist_state_kvs_id)

        # Attempt to retrieve the previously persisted state of the session pool
        previous_state: dict | None = await self._kvs.get_value(key=self._persist_state_key)

        # Restore each session from the stored state if previous state exists
        if previous_state:
            for session_id, session_state in previous_state['sessions'].items():
                session = Session(**session_state)
                if session.is_usable:
                    self._sessions[session_id] = session

        # If no previous state is available, create and initialize new sessions
        else:
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
        # If the pool is not full, create a new session
        if len(self._sessions) < self._max_pool_size:
            await self._create_new_session()

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
        new_session = Session(self._session_settings)
        self._sessions[new_session.id] = new_session
        return new_session

    def _get_random_session(self) -> Session:
        """Get a random session from the pool."""
        keys = list(self._sessions.keys())
        if not keys:
            raise ValueError('No sessions available in the pool.')
        key = random.choice(keys)
        return self._sessions[key]

    def _remove_retired_sessions(self) -> None:
        """Removes retired sessions from the pool."""
        self._sessions = {session_id: session for session_id, session in self._sessions.items() if session.is_usable}

    async def _persist_state(self, event_data: EventPersistStateData) -> None:
        """Persist the state of the session pool in the key value store."""
        logger.debug(f'Persisting state of the session pool (event_data={event_data}).')

        if not self._kvs:
            logger.warning('Key-value store is not initialized, did you forget to call __aenter__?')
            return

        await self._kvs.set_value(key=self._persist_state_key, value=self.state)
