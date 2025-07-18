from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stagehand import Stagehand, StagehandPage

if TYPE_CHECKING:
    from types import TracebackType


class CrawleeStagehandPage:
    """StagehandPage wrapper for Crawlee."""

    def __init__(self, page: StagehandPage) -> None:
        self._page = page

    async def goto(
        self,
        url: str,
        *,
        referer: str | None = None,
        timeout: int | None = None,
        wait_until: str | None = None,
    ) -> Any:
        """Navigate to the specified URL."""
        # Override goto to return navigation result that `PlaywrightCrawler` expects
        return await self._page._page.goto(  # noqa: SLF001
            url,
            referer=referer,
            timeout=timeout,
            wait_until=wait_until,
        )

    def __getattr__(self, name: str) -> Any:
        """Delegate all other methods to the underlying StagehandPage."""
        return getattr(self._page, name)

    async def __aenter__(self) -> CrawleeStagehandPage:
        """Enter the context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self._page.close()


class CrawleeStagehand(Stagehand):
    """Stagehand wrapper for Crawlee to disable the launch of Playwright."""

    async def init(self) -> None:
        # Skip Stagehand's own Playwright initialization
        # Let Crawlee's PlaywrightBrowserPlugin manage the browser lifecycle
        self._initialized = True
