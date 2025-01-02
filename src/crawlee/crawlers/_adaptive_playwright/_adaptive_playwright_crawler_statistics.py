from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Annotated, Any, cast

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.statistics import Statistics, StatisticsState

if TYPE_CHECKING:
    from logging import Logger

    from typing_extensions import Self

    from crawlee.storages import KeyValueStore


@docs_group('Data structures')
class PredictorState(BaseModel):
    model_config = ConfigDict(populate_by_name=True, ser_json_inf_nan='constants')

    http_only_request_handler_runs: Annotated[int, Field(alias='http_only_request_handler_runs')] = 0
    browser_request_handler_runs: Annotated[int, Field(alias='browser_request_handler_runs')] = 0
    rendering_type_mispredictions: Annotated[int, Field(alias='rendering_type_mispredictions')] = 0

@docs_group('Classes')
class AdaptivePlaywrightCrawlerStatistics(Statistics[StatisticsState]):


    def __init__(self,*,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str = 'default',
        persist_state_key: str | None = None,
        key_value_store: KeyValueStore | None = None,
        log_message: str = 'Statistics',
        periodic_message_logger: Logger | None = None,
        log_interval: timedelta = timedelta(minutes=1),
        state_model: type[StatisticsState] = StatisticsState) -> None:
        self._predictor_state = PredictorState()
        super().__init__(persistence_enabled=persistence_enabled,
                         persist_state_kvs_name=persist_state_kvs_name,
                         persist_state_key=persist_state_key,
                         key_value_store=key_value_store,
                         log_message=log_message,
                         periodic_message_logger=periodic_message_logger,
                         log_interval=log_interval,
                         state_model=state_model)
        self._persist_predictor_state_key = self._get_default_persist_state_key() + '_PREDICTOR'

    @classmethod
    def from_statistics(cls, statistics: Statistics[StatisticsState]) -> Self:
        return cls(persistence_enabled=statistics._persistence_enabled,  # noqa:SLF001  # Accessing private member to create copy like-object.
                         persist_state_kvs_name=statistics._persist_state_kvs_name,  # noqa:SLF001  # Accessing private member to create copy like-object.
                         persist_state_key=statistics._persist_state_key,  # noqa:SLF001  # Accessing private member to create copy like-object.
                         key_value_store=statistics._key_value_store,  # noqa:SLF001  # Accessing private member to create copy like-object.
                         log_message=statistics._log_message,  # noqa:SLF001  # Accessing private member to create copy like-object.
                         periodic_message_logger=statistics._periodic_message_logger,  # noqa:SLF001  # Accessing private member to create copy like-object.
                         log_interval=statistics._log_interval,  # noqa:SLF001  # Accessing private member to create copy like-object.
                         state_model=statistics._state_model,  # noqa:SLF001  # Accessing private member to create copy like-object.
                   )

    def track_http_only_request_handler_runs(self) -> None:
        self._predictor_state.http_only_request_handler_runs += 1

    def track_browser_request_handler_runs(self) -> None:
        self._predictor_state.browser_request_handler_runs += 1

    def track_rendering_type_mispredictions(self) -> None:
        self._predictor_state.rendering_type_mispredictions += 1

    @override
    async def _persist_other_statistics(self, key_value_store: KeyValueStore) -> None:
        """Persist state of predictor."""
        await key_value_store.set_value(
            self._persist_predictor_state_key,
            self._predictor_state.model_dump(mode='json', by_alias=True),
            'application/json',
        )


    @override
    async def _load_other_statistics(self, key_value_store: KeyValueStore) -> None:
        """Load state of predictor."""
        stored_state = await key_value_store.get_value(self._persist_predictor_state_key, cast(Any, {}))
        self._predictor_state = self._predictor_state.__class__.model_validate(stored_state)

