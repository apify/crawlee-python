from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import override

from crawlee._utils.console import make_table
from crawlee._utils.docs import docs_group
from crawlee._utils.models import timedelta_ms


@dataclass(frozen=True)
@docs_group('Data structures')
class FinalStatistics:
    """Statistics about a crawler run."""

    requests_finished: int
    requests_failed: int
    retry_histogram: list[int]
    request_avg_failed_duration: timedelta | None
    request_avg_finished_duration: timedelta | None
    requests_finished_per_minute: float
    requests_failed_per_minute: float
    request_total_duration: timedelta
    requests_total: int
    crawler_runtime: timedelta

    def to_table(self) -> str:
        """Print out the Final Statistics data as a table."""
        str_dict = {k: v.total_seconds() if isinstance(v, timedelta) else v for k, v in asdict(self).items()}

        return make_table([(str(k), str(v)) for k, v in str_dict.items()], width=60)

    @override
    def __str__(self) -> str:
        return json.dumps(
            {k: v.total_seconds() if isinstance(v, timedelta) else v for k, v in asdict(self).items()},
        )


@docs_group('Data structures')
class StatisticsState(BaseModel):
    """Statistic data about a crawler run."""

    model_config = ConfigDict(populate_by_name=True, ser_json_inf_nan='constants')

    requests_finished: Annotated[int, Field(alias='requestsFinished')] = 0
    requests_failed: Annotated[int, Field(alias='requestsFailed')] = 0
    requests_retries: Annotated[int, Field(alias='requestsRetries')] = 0
    requests_failed_per_minute: Annotated[float, Field(alias='requestsFailedPerMinute')] = 0
    requests_finished_per_minute: Annotated[float, Field(alias='requestsFinishedPerMinute')] = 0
    request_min_duration: Annotated[timedelta_ms | None, Field(alias='requestMinDurationMillis')] = None
    request_max_duration: Annotated[timedelta_ms | None, Field(alias='requestMaxDurationMillis')] = None
    request_total_failed_duration: Annotated[timedelta_ms, Field(alias='requestTotalFailedDurationMillis')] = (
        timedelta()
    )
    request_total_finished_duration: Annotated[timedelta_ms, Field(alias='requestTotalFinishedDurationMillis')] = (
        timedelta()
    )
    crawler_started_at: Annotated[datetime | None, Field(alias='crawlerStartedAt')] = None
    crawler_last_started_at: Annotated[datetime | None, Field(alias='crawlerLastStartTimestamp')] = None
    crawler_finished_at: Annotated[datetime | None, Field(alias='crawlerFinishedAt')] = None
    crawler_runtime: Annotated[timedelta_ms, Field(alias='crawlerRuntimeMillis')] = timedelta()
    errors: dict[str, Any] = Field(default_factory=dict)
    retry_errors: dict[str, Any] = Field(alias='retryErrors', default_factory=dict)
    requests_with_status_code: dict[str, int] = Field(alias='requestsWithStatusCode', default_factory=dict)
    stats_persisted_at: Annotated[datetime | None, Field(alias='statsPersistedAt')] = None


@docs_group('Data structures')
class StatisticsPersistedState(BaseModel):
    """Additional statistic data to be stored in the persisted state."""

    model_config = ConfigDict(populate_by_name=True)

    request_retry_histogram: Annotated[list[int], Field(alias='requestRetryHistogram')]
    stats_id: Annotated[int, Field(alias='statsId')]
    request_avg_failed_duration: Annotated[timedelta_ms | None, Field(alias='requestAvgFailedDurationMillis')]
    request_avg_finished_duration: Annotated[timedelta_ms | None, Field(alias='requestAvgFinishedDurationMillis')]
    request_total_duration: Annotated[timedelta_ms, Field(alias='requestTotalDurationMillis')]
    requests_total: Annotated[int, Field(alias='requestsTotal')]
    crawler_last_started_at: Annotated[datetime, Field(alias='crawlerLastStartTimestamp')]
    stats_persisted_at: Annotated[datetime, Field(alias='statsPersistedAt')]
