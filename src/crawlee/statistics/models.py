# ruff: noqa: TCH003
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from pydantic import BaseModel


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


class StatisticsModel(BaseModel):
    pass
