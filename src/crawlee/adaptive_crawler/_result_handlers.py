from __future__ import annotations

import itertools
from dataclasses import dataclass


def default_result_checker():
    return True

def default_result_comparator(result_1: SubCrawlerResult, result_2: SubCrawlerResult):
    for request_1, request_2 in itertools.zip_longest(result_1.add_request_kwargs["requests"], result_2.add_request_kwargs["requests"]):
        if request_1 != request_2:
            print("Different requests")
            # PlayWright can produce links with extra arguments compared to pure BS. Should it be considered as difference?
            # https://sdk.apify.com/docs/guides/getting-started
            # https://sdk.apify.com/docs/guides/getting-started?__hsfp=1136113150&__hssc=7591405.1.1735494277124&__hstc=7591405.e2b9302ed00c5bfaee3a870166792181.1735494277124.1735494277124.1735494277124.1

    return result_1 == result_2

class RenderingTypePredictor:
    ...


@dataclass(frozen=True)
class SubCrawlerResult:
    """Result produced by sub crawler."""
    push_data_kwargs: dict | None = None
    add_request_kwargs: dict | None = None
    links: list | None = None
    state: any | None = None
