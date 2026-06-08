from __future__ import annotations

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from crawlee._utils.docs import docs_group
from crawlee.statistics import StatisticsState


@docs_group('Statistics')
class AdaptivePlaywrightCrawlerStatisticState(StatisticsState):
    """Statistic data about a crawler run with additional information related to adaptive crawling."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, ser_json_inf_nan='constants'
    )

    http_only_request_handler_runs: int = 0
    """Number representing how many times static http based crawling was used."""

    browser_request_handler_runs: int = 0
    """Number representing how many times browser based crawling was used."""

    rendering_type_mispredictions: int = 0
    """Number representing how many times the predictor gave incorrect prediction."""
