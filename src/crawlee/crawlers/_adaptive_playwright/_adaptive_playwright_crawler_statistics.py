from __future__ import annotations

from typing import Annotated

from pydantic import ConfigDict, Field

from crawlee._utils.docs import docs_group
from crawlee.statistics import StatisticsState


@docs_group('Data structures')
class AdaptivePlaywrightCrawlerStatisticState(StatisticsState):
    """Statistic data about a crawler run with additional information related to adaptive crawling."""

    model_config = ConfigDict(populate_by_name=True, ser_json_inf_nan='constants')

    http_only_request_handler_runs: Annotated[int, Field(alias='http_only_request_handler_runs')] = 0
    """Number representing how many times static http based crawling was used."""

    browser_request_handler_runs: Annotated[int, Field(alias='browser_request_handler_runs')] = 0
    """Number representing how many times browser based crawling was used."""

    rendering_type_mispredictions: Annotated[int, Field(alias='rendering_type_mispredictions')] = 0
    """Number representing how many times the predictor gave incorrect prediction."""
