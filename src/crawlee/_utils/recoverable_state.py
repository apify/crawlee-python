from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar

from pydantic import BaseModel, ValidationError

from crawlee._utils.raise_if_too_many_kwargs import raise_if_too_many_kwargs
from crawlee._utils.wait import wait_for
from crawlee.events._types import Event, EventPersistStateData

if TYPE_CHECKING:
    import logging
    from collections.abc import Awaitable, Callable, Coroutine

    from crawlee.events import EventManager
    from crawlee.storages import KeyValueStore

TStateModel = TypeVar('TStateModel', bound=BaseModel)
T = TypeVar('T')


class RecoverableState(Generic[TStateModel]):
    """A class for managing persistent recoverable state using a Pydantic model.

    This class facilitates state persistence to a `KeyValueStore`, allowing data to be saved and retrieved
    across migrations or restarts. It manages the loading, saving, and resetting of state data,
    with optional persistence capabilities.

    The state is represented by a Pydantic model that can be serialized to and deserialized from JSON.
    The class automatically hooks into the event system to persist state when needed.

    Type Parameters:
        TStateModel: A Pydantic BaseModel type that defines the structure of the state data.
                     Typically, it should be inferred from the `default_state` constructor parameter.
    """

    def __init__(
        self,
        *,
        default_state: TStateModel,
        persist_state_key: str,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str | None = None,
        persist_state_kvs_id: str | None = None,
        persist_state_kvs_factory: Callable[[], Coroutine[None, None, KeyValueStore]] | None = None,
        persistence_timeout: timedelta = timedelta(seconds=60),
        logger: logging.Logger,
    ) -> None:
        """Initialize a new recoverable state object.

        Args:
            default_state: The default state model instance to use when no persisted state is found.
                A deep copy is made each time the state is used.
            persist_state_key: The key under which the state is stored in the KeyValueStore
            persistence_enabled: Flag to enable or disable state persistence.
            persist_state_kvs_name: The name of the KeyValueStore to use for persistence.
                If neither a name nor and id are supplied, the default store will be used.
            persist_state_kvs_id: The identifier of the KeyValueStore to use for persistence.
                If neither a name nor and id are supplied, the default store will be used.
            persist_state_kvs_factory: Factory that can be awaited to create KeyValueStore to use for persistence. If
                not provided, a system-wide KeyValueStore will be used, based on service locator configuration.
            persistence_timeout: Time limit for a single load, save or clear of the persisted state.
            logger: A logger instance for logging operations related to state persistence
        """
        raise_if_too_many_kwargs(
            persist_state_kvs_name=persist_state_kvs_name,
            persist_state_kvs_id=persist_state_kvs_id,
            persist_state_kvs_factory=persist_state_kvs_factory,
        )
        if not persist_state_kvs_factory:
            logger.debug(
                'No explicit key_value_store set for recoverable state. Recovery will use a system-wide KeyValueStore '
                'based on service_locator configuration, potentially calling service_locator.set_storage_backend in '
                'the process. It is recommended to initialize RecoverableState with explicit key_value_store to avoid '
                'global side effects.'
            )

        self._default_state = default_state
        self._state_type: type[TStateModel] = self._default_state.__class__
        self._state: TStateModel | None = None
        self._initialized = False
        self._record_loaded = False
        self._lock = asyncio.Lock()
        self._persistence_enabled = persistence_enabled
        self._persist_state_key = persist_state_key
        self._persistence_timeout = persistence_timeout
        if persist_state_kvs_factory is None:

            async def kvs_factory() -> KeyValueStore:
                from crawlee.storages import KeyValueStore  # noqa: PLC0415 avoid circular import

                return await KeyValueStore.open(name=persist_state_kvs_name, id=persist_state_kvs_id)

            self._persist_state_kvs_factory = kvs_factory
        else:
            self._persist_state_kvs_factory = persist_state_kvs_factory

        self._key_value_store: KeyValueStore | None = None
        self._log = logger

    async def initialize(self) -> TStateModel:
        """Initialize the recoverable state.

        If persistence is enabled, this method loads the saved state and registers the object to listen for
        PERSIST_STATE events. A state established beforehand by `reset` survives if there is no record to restore.

        Calling this again after `teardown` registers the listener again, but doesn't reload the record.

        Returns:
            The loaded state model

        Raises:
            ValidationError: If the persisted record is invalid. The record is kept, and every call raises again until
                it's fixed or cleared by `reset_store`.
        """
        async with self._lock:
            if self._initialized:
                return self.current_value

            if not self._record_loaded:
                await self._load_saved_state()

            # Registered only after a successful load, so that a failed one leaves no listener behind.
            if self._persistence_enabled:
                self._event_manager.on(event=Event.PERSIST_STATE, listener=self._persist_state_quietly)

            self._initialized = True
            return self.current_value

    async def teardown(self) -> None:
        """Clean up resources used by the recoverable state.

        If persistence is enabled, this method deregisters the object from PERSIST_STATE events and persists the
        current state one last time, logging a warning if that fails. The in-memory state is kept, and `initialize`
        can be called again.
        """
        async with self._lock:
            self._initialized = False

            if not self._persistence_enabled:
                return

            self._event_manager.off(event=Event.PERSIST_STATE, listener=self._persist_state_quietly)
            await self._persist_state_quietly()

    @property
    def current_value(self) -> TStateModel:
        """Get the current state."""
        if self._state is None:
            raise RuntimeError('Recoverable state has not yet been loaded - call initialize() or reset() first')

        return self._state

    @property
    def is_initialized(self) -> bool:
        """Check if `initialize` has finished loading the state and `teardown` wasn't called since."""
        return self._initialized

    def reset(self) -> None:
        """Reset the in-memory state to the default values, leaving any persisted record alone.

        Use `reset_store` to clear the persisted record as well.
        """
        self._state = self._default_state.model_copy(deep=True)

    async def reset_store(self) -> None:
        """Clear the persisted state record, leaving the in-memory state alone.

        Does nothing if persistence is disabled.

        Raises:
            RuntimeError: If the state is still being persisted on PERSIST_STATE events, which would write the record
                straight back. Call `teardown` first.
        """
        # The lock makes a call during a load wait for the listener that the load registers.
        async with self._lock:
            if not self._persistence_enabled:
                return

            if self._initialized:
                raise RuntimeError(
                    f"Cannot clear the state persisted under key '{self._persist_state_key}' while it is still being "
                    'persisted periodically - the next PERSIST_STATE event would write it straight back. Use reset() '
                    'to reset the state itself, or teardown() before clearing the record.'
                )

            key_value_store = await self._open_key_value_store()

            await self._with_timeout(
                lambda: key_value_store.set_value(self._persist_state_key, None), 'Clearing the persisted state'
            )

    async def persist_state(self, event_data: EventPersistStateData | None = None) -> None:
        """Persist the current state to the KeyValueStore.

        This method is typically called in response to a PERSIST_STATE event, but can also be called directly when
        needed. It does nothing if persistence is disabled, or before `initialize` has read the persisted record. A
        failed write raises from a direct call, while the persists on PERSIST_STATE events and in `teardown` log a
        warning instead.

        Args:
            event_data: Optional data associated with a PERSIST_STATE event
        """
        if not self._persistence_enabled or not self._record_loaded:
            return

        self._log.debug(
            'Persisting RecoverableState (model=%s, event_data=%s).', self._default_state.__class__.__name__, event_data
        )

        key_value_store = await self._open_key_value_store()

        await self._with_timeout(
            lambda: key_value_store.set_value(
                self._persist_state_key,
                self.current_value.model_dump(mode='json', by_alias=True),
                content_type='application/json',
            ),
            'Persisting the state',
        )

    @property
    def _event_manager(self) -> EventManager:
        # Import here to avoid circular imports.
        from crawlee import service_locator  # noqa: PLC0415

        return service_locator.get_event_manager()

    async def _persist_state_quietly(self, event_data: EventPersistStateData | None = None) -> None:
        try:
            await self.persist_state(event_data)
        except Exception:
            self._log.warning(f"Failed to persist the state under key '{self._persist_state_key}'.", exc_info=True)

    async def _open_key_value_store(self) -> KeyValueStore:
        if self._key_value_store is None:
            self._key_value_store = await self._persist_state_kvs_factory()

        return self._key_value_store

    async def _load_saved_state(self) -> None:
        stored_state = None
        if self._persistence_enabled:
            key_value_store = await self._open_key_value_store()
            stored_state = await self._with_timeout(
                lambda: key_value_store.get_value(self._persist_state_key), 'Loading the persisted state'
            )

        if stored_state is not None:
            try:
                self._state = self._state_type.model_validate(stored_state)
            except ValidationError as exc:
                exc.add_note(
                    f"The state persisted under key '{self._persist_state_key}' is invalid. Fix or delete the record "
                    'to start from the default state.'
                )
                raise
        elif self._state is None:
            self._state = self._default_state.model_copy(deep=True)

        # Set only once the load succeeds. A load that fails, is cancelled or finds an invalid record leaves the object
        # unwired, so that nothing is persisted over the record, and the next call loads it again.
        self._record_loaded = True

    async def _with_timeout(self, operation: Callable[[], Awaitable[T]], description: str) -> T:
        return await wait_for(
            operation,
            timeout=self._persistence_timeout,
            timeout_message=f"{description} under key '{self._persist_state_key}' timed out after "
            f'{self._persistence_timeout.total_seconds():g} seconds.',
            logger=self._log,
        )
