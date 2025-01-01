from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Annotated

from pydantic import Field

from crawlee._utils.docs import docs_group
from crawlee.statistics import Statistics, StatisticsState

if TYPE_CHECKING:
    from logging import Logger

    from typing_extensions import Self

    from crawlee.statistics._statistics import TStatisticsState
    from crawlee.storages import KeyValueStore


@docs_group('Data structures')
class AdaptivePlaywrightCrawlerStatisticState(StatisticsState):
    http_only_request_handler_runs: Annotated[int, Field(alias='http_only_request_handler_runs')] = 0
    browser_request_handler_runs: Annotated[int, Field(alias='browser_request_handler_runs')] = 0
    rendering_type_mispredictions: Annotated[int, Field(alias='rendering_type_mispredictions')] = 0

@docs_group('Classes')
class AdaptivePlaywrightCrawlerStatistics(Statistics[AdaptivePlaywrightCrawlerStatisticState]):

    def __init__(self,*,
        persistence_enabled: bool = False,
        persist_state_kvs_name: str = 'default',
        persist_state_key: str | None = None,
        key_value_store: KeyValueStore | None = None,
        log_message: str = 'Statistics',
        periodic_message_logger: Logger | None = None,
        log_interval: timedelta = timedelta(minutes=1),
        state_model: type[AdaptivePlaywrightCrawlerStatisticState] = AdaptivePlaywrightCrawlerStatisticState) -> None:
        super().__init__(persistence_enabled=persistence_enabled,
                         persist_state_kvs_name=persist_state_kvs_name,
                         persist_state_key=persist_state_key,
                         key_value_store=key_value_store,
                         log_message=log_message,
                         periodic_message_logger=periodic_message_logger,
                         log_interval=log_interval,
                         state_model=state_model)

    @classmethod
    def from_statistics(cls, statistics: Statistics[TStatisticsState]) -> Self:
        return cls(persistence_enabled=statistics._persistence_enabled,  # noqa:SLF001 # Statistics class would need refactoring beyond the scope of this change. TODO:
                         persist_state_kvs_name=statistics._persist_state_kvs_name,  # noqa:SLF001 # Statistics class would need refactoring beyond the scope of this change. TODO:
                         persist_state_key=statistics._persist_state_key,  # noqa:SLF001 # Statistics class would need refactoring beyond the scope of this change. TODO:
                         key_value_store=statistics._key_value_store,  # noqa:SLF001 # Statistics class would need refactoring beyond the scope of this change. TODO:
                         log_message=statistics._log_message,  # noqa:SLF001 # Statistics class would need refactoring beyond the scope of this change. TODO:
                         periodic_message_logger=statistics._periodic_message_logger,  # noqa:SLF001 # Statistics class would need refactoring beyond the scope of this change. TODO:
                         log_interval=statistics._log_interval,  # noqa:SLF001 # Statistics class would need refactoring beyond the scope of this change. TODO:
                         state_model=AdaptivePlaywrightCrawlerStatisticState)

    def track_http_only_request_handler_runs(self) -> None:
        self.state.http_only_request_handler_runs += 1  # type:ignore[attr-defined] # Statistics class would need refactoring beyond the scope of this change. TODO:

    def track_browser_request_handler_runs(self) -> None:
        self.state.browser_request_handler_runs += 1  # type:ignore[attr-defined] # Statistics class would need refactoring beyond the scope of this change. TODO:

    def track_rendering_type_mispredictions(self) -> None:
        self.state.rendering_type_mispredictions += 1  # type:ignore[attr-defined] # Statistics class would need refactoring beyond the scope of this change. TODO:

