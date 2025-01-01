from typing import Annotated

from pydantic import Field
from typing_extensions import Self, override

from crawlee._utils.docs import docs_group
from crawlee.statistics import Statistics, StatisticsState


@docs_group('Data structures')
class AdaptivePlaywrightCrawlerStatisticState(StatisticsState):
    http_only_request_handler_runs: Annotated[int, Field(alias='http_only_request_handler_runs')] = 0
    browser_request_handler_runs: Annotated[int, Field(alias='browser_request_handler_runs')] = 0
    rendering_type_mispredictions: Annotated[int, Field(alias='rendering_type_mispredictions')] = 0

@docs_group('Classes')
class AdaptivePlaywrightCrawlerStatistics(Statistics[AdaptivePlaywrightCrawlerStatisticState]):


    def __init__(self, *args, **kwargs) -> None:
        if 'state_model' not in kwargs:
            kwargs['state_model'] = AdaptivePlaywrightCrawlerStatisticState
        super().__init__(*args, **kwargs)

    @classmethod
    def from_statistics(cls, statistics: Statistics) -> Self:
        pass


    @override
    def reset(self) -> None:
        super().reset()
        self.state.http_only_request_handler_runs = 0
        self.state.browser_request_Handler_runs = 0
        self.state.rendering_type_mispredictions = 0

    def track_http_only_request_handler_runs(self) -> None:
        self.state.http_only_request_handler_runs += 1

    def track_browser_request_handler_runs(self) -> None:
        self.state.browser_request_Handler_runs += 1

    def track_rendering_type_mispredictions(self) -> None:
        self.state.rendering_type_mispredictions += 1

