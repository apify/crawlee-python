from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

from pydantic import BaseModel

from crawlee import service_locator
from crawlee.events._types import Event, EventPersistStateData
from crawlee.storages._key_value_store import KeyValueStore

if TYPE_CHECKING:
    import logging

TStateModel = TypeVar('TStateModel', bound=BaseModel)


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
        logger: logging.Logger,
    ) -> None:
        """Initialize a new recoverable state object.

        Args:
            default_state: The default state model instance to use when no persisted state is found.
                           A deep copy is made each time the state is used.
            persist_state_key: The key under which the state is stored in the KeyValueStore
            persistence_enabled: Flag to enable or disable state persistence
            persist_state_kvs_name: The name of the KeyValueStore to use for persistence.
                                    If neither a name nor and id are supplied, the default store will be used.
            persist_state_kvs_id: The identifier of the KeyValueStore to use for persistence.
                                    If neither a name nor and id are supplied, the default store will be used.
            logger: A logger instance for logging operations related to state persistence
        """
        self._default_state = default_state
        self._state_type: type[TStateModel] = self._default_state.__class__
        self._state: TStateModel | None = None
        self._persistence_enabled = persistence_enabled
        self._persist_state_key = persist_state_key
        self._persist_state_kvs_name = persist_state_kvs_name
        self._persist_state_kvs_id = persist_state_kvs_id
        self._key_value_store: KeyValueStore | None = None
        self._log = logger

    async def initialize(self) -> TStateModel:
        """Initialize the recoverable state.

        This method must be called before using the recoverable state. It loads the saved state
        if persistence is enabled and registers the object to listen for PERSIST_STATE events.

        Returns:
            The loaded state model
        """
        if not self._persistence_enabled:
            self._state = self._default_state.model_copy(deep=True)
            return self.current_value

        self._key_value_store = await KeyValueStore.open(
            name=self._persist_state_kvs_name, id=self._persist_state_kvs_id
        )

        await self._load_saved_state()

        event_manager = service_locator.get_event_manager()
        event_manager.on(event=Event.PERSIST_STATE, listener=self.persist_state)

        return self.current_value

    async def teardown(self) -> None:
        """Clean up resources used by the recoverable state.

        If persistence is enabled, this method deregisters the object from PERSIST_STATE events
        and persists the current state one last time.
        """
        if not self._persistence_enabled:
            return

        event_manager = service_locator.get_event_manager()
        event_manager.off(event=Event.PERSIST_STATE, listener=self.persist_state)
        await self.persist_state()

    @property
    def current_value(self) -> TStateModel:
        """Get the current state."""
        if self._state is None:
            raise RuntimeError('Recoverable state has not yet been loaded')

        return self._state

    async def reset(self) -> None:
        """Reset the state to the default values and clear any persisted state.

        Resets the current state to the default state and, if persistence is enabled,
        clears the persisted state from the KeyValueStore.
        """
        self._state = self._default_state.model_copy(deep=True)

        if self._persistence_enabled:
            if self._key_value_store is None:
                raise RuntimeError('Recoverable state has not yet been initialized')

            await self._key_value_store.set_value(self._persist_state_key, None)

    async def persist_state(self, event_data: EventPersistStateData | None = None) -> None:
        """Persist the current state to the KeyValueStore.

        This method is typically called in response to a PERSIST_STATE event, but can also be called
        directly when needed.

        Args:
            event_data: Optional data associated with a PERSIST_STATE event
        """
        self._log.debug(f'Persisting state of the Statistics (event_data={event_data}).')

        if self._key_value_store is None or self._state is None:
            raise RuntimeError('Recoverable state has not yet been initialized')

        if self._persistence_enabled:
            await self._key_value_store.set_value(
                self._persist_state_key,
                self._state.model_dump(mode='json', by_alias=True),
                'application/json',
            )

    async def _load_saved_state(self) -> None:
        if self._key_value_store is None:
            raise RuntimeError('Recoverable state has not yet been initialized')

        stored_state = await self._key_value_store.get_value(self._persist_state_key)
        if stored_state is None:
            self._state = self._default_state.model_copy(deep=True)
        else:
            self._state = self._state_type.model_validate(stored_state)
