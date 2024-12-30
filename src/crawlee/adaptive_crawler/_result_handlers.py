from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from typing_extensions import NotRequired, TypedDict, Unpack

from crawlee._request import BaseRequestData, Request
from crawlee._types import PushDataKwargs, EnqueueLinksKwargs, JsonSerializable


def default_result_comparator(result_1: SubCrawlerResult, result_2: SubCrawlerResult) -> bool:
    """Default comparator in TS compares only push data arguments."""
    # PlayWright can produce links with extra arguments compared to pure BS. Default comparator ignores this as it does
    # not consider add_requests_kwargs
    # https://sdk.apify.com/docs/guides/getting-started
    # https://sdk.apify.com/docs/guides/getting-started?__hsfp=1136113150&__hssc=7591405.1.1735494277124&__hstc=7591405.e2b9302ed00c5bfaee3a870166792181.1735494277124.1735494277124.1735494277124.1

    return result_1.push_data_kwargs == result_2.push_data_kwargs

class CrawlTypePredictor:
    """Not implemented yet."""


class _PushDataKwargs(PushDataKwargs):
    data: JsonSerializable
    dataset_id: NotRequired[str | None]
    dataset_name: NotRequired[str | None]

class _AddRequestsKwargs(EnqueueLinksKwargs):
    requests: Sequence[str | BaseRequestData | Request]

@dataclass(frozen=True)
class SubCrawlerResult:
    """Result produced by sub crawler."""
    push_data_kwargs: _PushDataKwargs | None = None
    add_request_kwargs: _AddRequestsKwargs | None = None
    state: Any | None = None
    exception: Exception | None = None

    @property
    def ok(self) -> bool:
        """Return true there was no exception in sub crawler."""
        return not self.exception
