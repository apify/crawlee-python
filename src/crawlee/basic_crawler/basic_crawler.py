# Inspiration: https://github.com/apify/crawlee/blob/v3.7.3/packages/basic-crawler/src/internals/basic-crawler.ts

from __future__ import annotations

from typing import Awaitable, Callable

from crawlee.autoscaling import AutoscaledPool
from crawlee.autoscaling.snapshotter import Snapshotter
from crawlee.autoscaling.system_status import SystemStatus
from crawlee.basic_crawler.router import Router
from crawlee.basic_crawler.types import BasicCrawlingContext


class BasicCrawler:
    """Provides a simple framework for parallel crawling of web pages.

    The URLs to crawl are fed either from a static list of URLs or from a dynamic queue of URLs enabling recursive
    crawling of websites.

    `BasicCrawler` is a low-level tool that requires the user to implement the page download and data extraction
    functionality themselves. If we want a crawler that already facilitates this functionality, we should consider using
    one of its subclasses.
    """

    def __init__(self: BasicCrawler, *, router: Callable[[BasicCrawlingContext], Awaitable] | None = None) -> None:
        if isinstance(router, Router):
            self._router = router
        elif router is not None:
            self._router = None
            self.router.default_handler(router)
        else:
            self._router = None

        self._pool = AutoscaledPool(
            system_status=SystemStatus(Snapshotter()),
            is_finished_function=self._is_finished_function,
            is_task_ready_function=self._is_task_ready_function,
            run_task_function=self._run_task_function,
        )

    @property
    def router(self: BasicCrawler) -> Router[BasicCrawlingContext]:
        """The router used to handle each individual crawling request."""
        if self._router is None:
            self._router = Router[BasicCrawlingContext]()

        return self._router

    @router.setter
    def router(self: BasicCrawler, router: Router[BasicCrawlingContext]) -> None:
        if self._router is not None:
            raise RuntimeError('A router is already set')

        self._router = router

    def _is_finished_function(self: BasicCrawler) -> bool:
        pass

    def _is_task_ready_function(self: BasicCrawler) -> bool:
        pass

    async def _run_task_function(self: BasicCrawler) -> None:
        pass
