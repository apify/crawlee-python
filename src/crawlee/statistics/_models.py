from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, PlainValidator, computed_field, field_serializer
from pydantic.alias_generators import to_camel
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

    model_config = ConfigDict(
        alias_generator=to_camel, validate_by_name=True, validate_by_alias=True, ser_json_inf_nan='constants'
    )
    # Statistics persisted by crawlee v1 store the ID as a number.
    stats_id: Annotated[str | None, Field(coerce_numbers_to_str=True)] = None

    requests_finished: int = 0
    requests_failed: int = 0
    requests_retries: int = 0
    requests_failed_per_minute: float = 0
    requests_finished_per_minute: float = 0
    request_min_duration: Annotated[timedelta_ms | None, Field(alias='requestMinDurationMillis')] = None
    request_max_duration: Annotated[timedelta_ms | None, Field(alias='requestMaxDurationMillis')] = None
    request_total_failed_duration: Annotated[timedelta_ms, Field(alias='requestTotalFailedDurationMillis')] = (
        timedelta()
    )
    request_total_finished_duration: Annotated[timedelta_ms, Field(alias='requestTotalFinishedDurationMillis')] = (
        timedelta()
    )
    crawler_started_at: datetime | None = None
    crawler_last_started_at: Annotated[datetime | None, Field(alias='crawlerLastStartTimestamp')] = None
    crawler_finished_at: datetime | None = None

    # Workaround for Pydantic and type checkers when using Annotated with default_factory
    if TYPE_CHECKING:
        errors: dict[str, Any] = {}
        retry_errors: dict[str, Any] = {}
        requests_with_status_code: dict[str, int] = {}
    else:
        errors: Annotated[dict[str, Any], Field(default_factory=dict)]
        retry_errors: Annotated[dict[str, Any], Field(default_factory=dict)]
        requests_with_status_code: Annotated[dict[str, int], Field(default_factory=dict)]

    stats_persisted_at: Annotated[datetime | None, PlainSerializer(lambda _: datetime.now(UTC))] = None
    request_retry_histogram: Annotated[
        dict[int, int],
        PlainValidator(lambda value: dict(enumerate(value)), json_schema_input_type=list[int]),
        PlainSerializer(
            lambda value: [value.get(i, 0) for i in range(max(value.keys(), default=0) + 1)],
            return_type=list[int],
        ),
    ] = {}

    accumulated_runtime: Annotated[timedelta_ms, Field(alias='crawlerRuntimeMillis')] = timedelta()
    """The runtime accumulated by the previous runs of the crawler, excluding the run in progress."""

    @property
    def crawler_runtime(self) -> timedelta:
        """The total runtime of the crawler, accumulated over all its runs."""
        if self.crawler_last_started_at is None:
            return self.accumulated_runtime
        if self.stats_persisted_at is not None and self.crawler_last_started_at <= self.stats_persisted_at:
            # A restored snapshot already checkpoints this run in `accumulated_runtime`, do not count it twice.
            return self.accumulated_runtime

        finished_at = self.crawler_finished_at or datetime.now(UTC)
        return self.accumulated_runtime + max(timedelta(), finished_at - self.crawler_last_started_at)

    @field_serializer('accumulated_runtime')
    def _serialize_accumulated_runtime(self, _value: timedelta) -> int:
        """Serialize the total runtime including the run in progress, so that every persist checkpoints it."""
        return round(self.crawler_runtime.total_seconds() * 1000)

    @computed_field(alias='requestTotalDurationMillis', return_type=timedelta_ms)
    @property
    def request_total_duration(self) -> timedelta:
        return self.request_total_finished_duration + self.request_total_failed_duration

    @computed_field(alias='requestAvgFailedDurationMillis', return_type=timedelta_ms | None)
    @property
    def request_avg_failed_duration(self) -> timedelta | None:
        return (self.request_total_failed_duration / self.requests_failed) if self.requests_failed else None

    @computed_field(alias='requestAvgFinishedDurationMillis', return_type=timedelta_ms | None)
    @property
    def request_avg_finished_duration(self) -> timedelta | None:
        return (self.request_total_finished_duration / self.requests_finished) if self.requests_finished else None

    @computed_field
    @property
    def requests_total(self) -> int:
        return self.requests_failed + self.requests_finished
