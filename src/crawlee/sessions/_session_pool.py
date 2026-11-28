# Inspiration: https://github.com/apify/crawlee/blob/v3.9.0/packages/core/src/session_pool/session_pool.ts

from __future__ import annotations

import asyncio
import inspect
import random
from collections.abc import Awaitable, Callable
from logging import getLogger
from typing import TYPE_CHECKING, Literal, cast, overload

from crawlee import service_locator
from crawlee._utils.context import ensure_context
from crawlee._utils.docs import docs_group
from crawlee._utils.recoverable_state import RecoverableState
from crawlee.sessions import Session, SessionOptions
from crawlee.sessions._models import SessionPoolModel

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee.events import EventManager
    from crawlee.proxy_configuration import ProxyConfiguration

logger = getLogger(__name__)

CreateSessionFunctionType = Callable[[SessionOptions], Session | Awaitable[Session]]


@docs_group('Session management')
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
        create_session_settings: SessionOptions | None = None,
        create_session_function: CreateSessionFunctionType | None = None,
        proxy_configuration: ProxyConfiguration | None = None,
        event_manager: EventManager | None = None,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str | None = None,
        persist_state_key: str = 'CRAWLEE_SESSION_POOL_STATE',
    ) -> None:
        """Initialize a new instance.

        Args:
            max_pool_size: Maximum number of sessions to maintain in the pool. You can add more sessions to the pool
                by using the `add_session` method.
            create_session_settings: Default options of the sessions that the pool creates.
            create_session_function: A function, sync or async, that creates a session from the given options. The pool
                calls it for every new session and for every session restored from the persisted state, with
                `create_session_settings` merged with the state of the restored session. Build the session from these
                options, so that a restored session keeps its state. Defaults to `Session(**options)`.
            proxy_configuration: Assigns proxies to the sessions that the pool creates or restores. Before calling
                `create_session_function`, the pool sets `proxy_info` in the options to the result of
                `proxy_configuration.new_proxy_info()` called with the `proxy_info` already in the options, such as the
                proxy of a restored session. A session added with `add_session` keeps its own `proxy_info`, but one
                without a proxy gets a new one when the pool restores it.
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
        self._session_settings = create_session_settings or SessionOptions()
        self._create_session_function = create_session_function or (lambda options: Session(**options))
        self._proxy_configuration = proxy_configuration
        self._persistence_enabled = persistence_enabled

        # Keeps concurrent `get_session` calls from overfilling the pool while a session is being created.
        self._lock = asyncio.Lock()

        # Set once the sessions of the persisted record are recreated, so an entry that fails before that retries it.
        self._restored_sessions_recreated = False

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

        try:
            state = await self._state.initialize()
            state.max_pool_size = self._max_pool_size
            self._remove_retired_sessions()

            if not self._restored_sessions_recreated:
                restored_sessions = dict[str, Session]()
                for session in state.sessions.values():
                    restored_session = await self._build_session(_get_session_options(session))
                    restored_sessions[restored_session.id] = restored_session
                state.sessions = restored_sessions
                self._restored_sessions_recreated = True
        except BaseException:
            try:
                if self._state.is_initialized:
                    await self._state.teardown()
            finally:
                self._active = False
            raise

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

        try:
            await self._state.teardown()
        finally:
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

        This is intended only for the cases when you want to add a session that was created outside of the pool.
        Otherwise, the pool will create new sessions automatically.

        Args:
            session: The session to add to the pool.
        """
        state = self._state.current_value

        if session.id in state.sessions:
            logger.warning('Session with ID %s already exists in the pool.', session.id)
            return
        state.sessions[session.id] = session

    @ensure_context
    async def get_session(self) -> Session:
        """Retrieve a session from the pool.

        While the pool has fewer than `max_pool_size` sessions, this method creates a new one. Once the pool is full,
        it picks a random session. If the random session is not usable, retired sessions are removed and a new session
        is created and returned.

        Returns:
            The session object.
        """
        async with self._lock:
            if self.session_count < self._max_pool_size:
                return await self._create_new_session()

            session = self._get_random_session()

            if session.is_usable:
                return session

            # If the random session is not usable, clean up and create a new session
            self._remove_retired_sessions()
            return await self._create_new_session()

    @ensure_context
    async def get_session_by_id(self, session_id: str) -> Session | None:
        """Retrieve a session by ID from the pool.

        It doesn't create sessions. If the session is not found or not usable, `None` is returned.

        Args:
            session_id: The ID of the session to retrieve.

        Returns:
            The session object if found and usable, otherwise `None`.
        """
        session = self._state.current_value.sessions.get(session_id)

        if not session:
            logger.warning('Session with ID %s not found.', session_id)
            return None

        if not session.is_usable:
            logger.warning('Session with ID %s is not usable.', session_id)
            return None

        return session

    def reset(self) -> None:
        """Discard all sessions in the pool, leaving the persisted record alone.

        Use `reset_store` to clear the persisted record as well.
        """
        self._state.reset()

    async def reset_store(self) -> None:
        """Clear the persisted pool record, leaving the in-memory sessions alone.

        Raises:
            RuntimeError: If the pool is active with persistence enabled, since the next persist would write the record
                straight back.
        """
        await self._state.reset_store()

    async def _build_session(self, options: SessionOptions) -> Session:
        """Build a session from `options` merged over `create_session_settings`."""
        merged_options = self._session_settings.copy()
        merged_options.update(options)

        if self._proxy_configuration:
            merged_options['proxy_info'] = await self._proxy_configuration.new_proxy_info(
                merged_options.get('proxy_info')
            )

        session = self._create_session_function(merged_options)
        if inspect.isawaitable(session):
            session = await session

        return session

    async def _create_new_session(self) -> Session:
        """Create a new session, add it to the pool and return it."""
        new_session = await self._build_session(SessionOptions())
        self._state.current_value.sessions[new_session.id] = new_session
        return new_session

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


def _get_session_options(session: Session) -> SessionOptions:
    """Get the options that recreate the given session."""
    state = session.get_state(as_dict=False)
    options = cast('SessionOptions', state.model_dump(exclude={'proxy_info'}))
    options['proxy_info'] = state.proxy_info
    return options
