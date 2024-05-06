# ruff: noqa: TCH003
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field


@dataclass(frozen=True)
class FinalStatistics:
    """Statistics about a crawler run."""

    requests_finished: int
    requests_failed: int
    retry_histogram: list[int]
    request_avg_failed_duration: timedelta
    request_avg_finished_duration: timedelta
    requests_finished_per_minute: float
    requests_failed_per_minute: float
    request_total_duration: timedelta
    requests_total: int
    crawler_runtime: timedelta


class StatisticsState(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    requests_finished: Annotated[int, Field(alias='requestsFinished')]
    requests_failed: Annotated[int, Field(alias='requestsFailed')]
    requests_retries: Annotated[int, Field(alias='requestsRetries')]
    requests_failed_per_minute: Annotated[float, Field(alias='requestsFailedPerMinute')]
    requests_finished_per_minute: Annotated[float, Field(alias='requestsFinishedPerMinute')]
    request_min_duration: Annotated[timedelta, Field(alias='requestMinDurationMillis')]
    request_max_duration: Annotated[timedelta, Field(alias='requestMaxDurationMillis')]
    request_total_failed_duration: Annotated[timedelta, Field(alias='requestTotalFailedDurationMillis')]
    request_total_finished_duration: Annotated[timedelta, Field(alias='requestTotalFinishedDurationMilles')]
    crawler_started_at: Annotated[datetime | None, Field(alias='crawlerStartedAt')]
    crawler_finished_at: Annotated[datetime | None, Field(alias='crawlerFinishedAt')]
    crawler_runtime: Annotated[timedelta, Field(alias='crawlerRuntime')]
    errors: dict[str, Any]
    retry_errors: Annotated[dict[str, Any], Field(alias='retryErrors')]
    requests_with_status_code: Annotated[dict[str, int], Field(alias='requestsWithStatusCode')]
    stats_persisted_at: Annotated[datetime | None, Field(alias='statsPersistedAt')]


class StatisticsPersistedState(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    request_retry_histogram: Annotated[list[int], Field(alias='requestRetryHistogram')]
    stats_id: Annotated[int, Field(alias='statsId')]
    request_avg_failed_duration: Annotated[timedelta, Field(alias='requestAvgFailedDurationMillis')]
    request_avg_finished_duration: Annotated[timedelta, Field(alias='requestAvgFinishedDurationMillis')]
    request_total_duration: Annotated[int, Field(alias='requestTotalDurationMillis')]
    requests_total: Annotated[int, Field(alias='requestsTotal')]
    crawler_last_started_at: Annotated[datetime, Field(alias='crawlerLastStartTimestamp')]
    stats_persisted_at: Annotated[datetime, Field(alias='statsPersistedAt')]
