from __future__ import annotations

from typing import Annotated

from pydantic import ConfigDict, Field
from pydantic.alias_generators import to_camel

from crawlee._utils.docs import docs_group
from crawlee.statistics import StatisticsState


@docs_group('Statistics')
class AdaptivePlaywrightCrawlerStatisticState(StatisticsState):
    """Statistic data about a crawler run with additional information related to adaptive crawling."""

    model_config = ConfigDict(
        validate_by_name=True, validate_by_alias=True, alias_generator=to_camel, ser_json_inf_nan='constants'
    )

    # These fields are serialized with snake_case keys, so they keep explicit aliases that override the
    # camelCase alias generator.
    http_only_request_handler_runs: Annotated[int, Field(alias='http_only_request_handler_runs')] = 0
    """Number representing how many times static http based crawling was used."""

    browser_request_handler_runs: Annotated[int, Field(alias='browser_request_handler_runs')] = 0
    """Number representing how many times browser based crawling was used."""

    rendering_type_mispredictions: Annotated[int, Field(alias='rendering_type_mispredictions')] = 0
    """Number representing how many times the predictor gave incorrect prediction."""
