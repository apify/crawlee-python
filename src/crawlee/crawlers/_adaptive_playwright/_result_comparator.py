from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crawlee._types import RequestHandlerRunResult


@dataclass(frozen=True)
class SubCrawlerRun:
    result: RequestHandlerRunResult | None= None
    exception: Exception | None= None


def default_result_comparator(result_1: RequestHandlerRunResult, result_2: RequestHandlerRunResult) -> bool:

    # PlayWright can produce links with extra arguments compared to pure BS. Default comparator ignores this as it does
    # not consider add_requests_kwargs
    # https://sdk.apify.com/docs/guides/getting-started
    # https://sdk.apify.com/docs/guides/getting-started?__hsfp=1136113150&__hssc=7591405.1.1735494277124&__hstc=7591405.e2b9302ed00c5bfaee3a870166792181.1735494277124.1735494277124.1735494277124.1

    return (
        (result_1.push_data_calls == result_2.push_data_calls) and
        (result_1.add_requests_calls == result_2.add_requests_calls) and
        (result_1.key_value_store_changes == result_2.key_value_store_changes))
