from __future__ import annotations

import itertools
from dataclasses import dataclass


def default_result_checker(result: SubCrawlerResult):
    return True

def default_result_comparator(result_1: SubCrawlerResult, result_2: SubCrawlerResult):
    # Default comparator in TS compares only push data

    # PlayWright can produce links with extra arguments compared to pure BS. Default comparator ignores this as it does not consider add_requests_kwargs
    # https://sdk.apify.com/docs/guides/getting-started
    # https://sdk.apify.com/docs/guides/getting-started?__hsfp=1136113150&__hssc=7591405.1.1735494277124&__hstc=7591405.e2b9302ed00c5bfaee3a870166792181.1735494277124.1735494277124.1735494277124.1

    return result_1.push_data_kwargs == result_2.push_data_kwargs

class RenderingTypePredictor:
    ...


@dataclass(frozen=True)
class SubCrawlerResult:
    """Result produced by sub crawler."""
    push_data_kwargs: dict | None = None
    add_request_kwargs: dict | None = None
    state: any | None = None
    exception: Exception | None = None

    @property
    def ok(self) -> bool:
        return not self.exception
