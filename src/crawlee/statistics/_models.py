from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, PlainValidator, computed_field
from typing_extensions import override

from crawlee._utils.console import make_table
from crawlee._utils.docs import docs_group
from crawlee._utils.models import timedelta_ms
from crawlee._utils.time import format_duration

_STATISTICS_TABLE_WIDTH = 100


@dataclass(frozen=True)
@docs_group('Statistics')
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
        formatted_dict = {}
        for k, v in asdict(self).items():
            if isinstance(v, timedelta):
                formatted_dict[k] = format_duration(v)
            else:
                formatted_dict[k] = v

        return make_table([(str(k), str(v)) for k, v in formatted_dict.items()], width=_STATISTICS_TABLE_WIDTH)

    def to_dict(self) -> dict[str, float | int | list[int]]:
        return {k: v.total_seconds() if isinstance(v, timedelta) else v for k, v in asdict(self).items()}

    @override
    def __str__(self) -> str:
        return json.dumps(
            {k: v.total_seconds() if isinstance(v, timedelta) else v for k, v in asdict(self).items()},
        )


@docs_group('Statistics')
class StatisticsState(BaseModel):
    """Statistic data about a crawler run."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True, ser_json_inf_nan='constants')
    stats_id: Annotated[int | None, Field(alias='statsId')] = None

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
    stats_persisted_at: Annotated[
        datetime | None, Field(alias='statsPersistedAt'), PlainSerializer(lambda _: datetime.now(timezone.utc))
    ] = None
    request_retry_histogram: Annotated[
        dict[int, int],
        Field(alias='requestRetryHistogram'),
        PlainValidator(lambda value: dict(enumerate(value)), json_schema_input_type=list[int]),
        PlainSerializer(
            lambda value: [value.get(i, 0) for i in range(max(value.keys(), default=0) + 1)],
            return_type=list[int],
        ),
    ] = {}

    @computed_field(alias='requestTotalDurationMillis', return_type=timedelta_ms)  # type: ignore[prop-decorator]
    @property
    def request_total_duration(self) -> timedelta:
        return self.request_total_finished_duration + self.request_total_failed_duration

    @computed_field(alias='requestAvgFailedDurationMillis', return_type=timedelta_ms | None)  # type: ignore[prop-decorator]
    @property
    def request_avg_failed_duration(self) -> timedelta | None:
        return (self.request_total_failed_duration / self.requests_failed) if self.requests_failed else None

    @computed_field(alias='requestAvgFinishedDurationMillis', return_type=timedelta_ms | None)  # type: ignore[prop-decorator]
    @property
    def request_avg_finished_duration(self) -> timedelta | None:
        return (self.request_total_finished_duration / self.requests_finished) if self.requests_finished else None

    @computed_field(alias='requestsTotal')  # type: ignore[prop-decorator]
    @property
    def requests_total(self) -> int:
        return self.requests_failed + self.requests_finished
