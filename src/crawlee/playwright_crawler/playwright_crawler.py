from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import Unpack

from crawlee.basic_crawler import (
    BasicCrawler,
    BasicCrawlerOptions,
    BasicCrawlingContext,
    ContextPipeline,
)
from crawlee.playwright_crawler.types import PlaywrightCrawlingContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class PlaywrightCrawler(BasicCrawler[PlaywrightCrawlingContext]):
    """A crawler that fetches the request URL using `Playwright`."""

    def __init__(
        self,
        **kwargs: Unpack[BasicCrawlerOptions[PlaywrightCrawlingContext]],
    ) -> None:
        """Create a new instance.

        Args:
            kwargs: Arguments to be forwarded to the underlying BasicCrawler
        """
        kwargs['use_browser_pool'] = True
        kwargs['_context_pipeline'] = ContextPipeline().compose(self._page_goto)
        super().__init__(**kwargs)

    async def _page_goto(
        self,
        context: BasicCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, None]:
        crawlee_page = await self._browser_pool.new_page()
        await crawlee_page.page.goto(context.request.url)

        yield PlaywrightCrawlingContext(
            request=context.request,
            session=context.session,
            send_request=context.send_request,
            add_requests=context.add_requests,
            proxy_info=context.proxy_info,
            page=crawlee_page.page,
        )
