from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Annotated

from pydantic import ConfigDict, Field

from crawlee._utils.docs import docs_group
from crawlee.statistics import Statistics, StatisticsState

if TYPE_CHECKING:
    from logging import Logger

    from typing_extensions import Self

    from crawlee.storages import KeyValueStore


@docs_group('Data structures')
class AdaptivePlaywrightCrawlerStatisticState(StatisticsState):
    model_config = ConfigDict(populate_by_name=True, ser_json_inf_nan='constants')

    http_only_request_handler_runs: Annotated[int, Field(alias='http_only_request_handler_runs')] = 0
    browser_request_handler_runs: Annotated[int, Field(alias='browser_request_handler_runs')] = 0
    rendering_type_mispredictions: Annotated[int, Field(alias='rendering_type_mispredictions')] = 0


@docs_group('Classes')
class AdaptivePlaywrightCrawlerStatistics(Statistics[AdaptivePlaywrightCrawlerStatisticState]):
    def __init__(
        self,
        *,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str = 'default',
        persist_state_key: str | None = None,
        key_value_store: KeyValueStore | None = None,
        log_message: str = 'Statistics',
        periodic_message_logger: Logger | None = None,
        log_interval: timedelta = timedelta(minutes=1),
    ) -> None:
        super().__init__(
            persistence_enabled=persistence_enabled,
            persist_state_kvs_name=persist_state_kvs_name,
            persist_state_key=persist_state_key,
            key_value_store=key_value_store,
            log_message=log_message,
            periodic_message_logger=periodic_message_logger,
            log_interval=log_interval,
            state_model=AdaptivePlaywrightCrawlerStatisticState,
        )

    @classmethod
    def from_statistics(cls, statistics: Statistics) -> Self:
        return cls(
            persistence_enabled=statistics._persistence_enabled,  # noqa:SLF001  # Accessing private member to create copy like-object.
            persist_state_kvs_name=statistics._persist_state_kvs_name,  # noqa:SLF001  # Accessing private member to create copy like-object.
            persist_state_key=statistics._persist_state_key,  # noqa:SLF001  # Accessing private member to create copy like-object.
            key_value_store=statistics._key_value_store,  # noqa:SLF001  # Accessing private member to create copy like-object.
            log_message=statistics._log_message,  # noqa:SLF001  # Accessing private member to create copy like-object.
            periodic_message_logger=statistics._periodic_message_logger,  # noqa:SLF001  # Accessing private member to create copy like-object.
            log_interval=statistics._log_interval,  # noqa:SLF001  # Accessing private member to create copy like-object.
        )
