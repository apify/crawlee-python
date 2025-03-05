from __future__ import annotations

import asyncio
import shutil
import tempfile
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any

from playwright.async_api import Browser
from typing_extensions import override

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext, BrowserType, CDPSession, Page

logger = getLogger(__name__)


class PlaywrightPersistentBrowser(Browser):
    """A wrapper for Playwright's `Browser` that operates with a persistent context.

    It utilizes Playwright's persistent browser context feature, maintaining user data across sessions.
    While it follows the same interface as Playwright's `Browser` class, there is no abstract base class
    enforcing this. There is a limitation that only a single persistent context is allowed.
    """

    _TMP_DIR_PREFIX = 'apify-playwright-firefox-taac-'

    def __init__(
        self,
        browser_type: BrowserType,
        user_data_dir: str | Path | None,
        browser_launch_options: dict[str, Any],
    ) -> None:
        self._browser_type = browser_type
        self._browser_launch_options = browser_launch_options
        self._user_data_dir = user_data_dir
        self._temp_dir: Path | None = None

        self._context: BrowserContext | None = None
        self._is_connected = True

    @property
    def browser_type(self) -> BrowserType:
        return self._browser_type

    @property
    def contexts(self) -> list[BrowserContext]:
        return [self._context] if self._context else []

    def is_connected(self) -> bool:
        return self._is_connected

    async def new_context(self, **context_options: Any) -> BrowserContext:
        """Create persistent context instead of regular one. Merge launch options with context options."""
        if self._context:
            raise RuntimeError('Persistent browser can have only one context')

        launch_options = self._browser_launch_options | context_options

        if self._user_data_dir:
            user_data_dir = self._user_data_dir
        else:
            user_data_dir = tempfile.mkdtemp(prefix=self._TMP_DIR_PREFIX)
            self._temp_dir = Path(user_data_dir)

        self._context = await self._browser_type.launch_persistent_context(
            user_data_dir=user_data_dir, **launch_options
        )

        if self._temp_dir:
            self._context.on('close', self._delete_temp_dir)

        return self._context

    async def _delete_temp_dir(self, _: BrowserContext | None) -> None:
        if self._temp_dir and self._temp_dir.exists():
            await asyncio.to_thread(shutil.rmtree, self._temp_dir, ignore_errors=True)

    @override
    async def close(self, **kwargs: Any) -> None:
        """Close browser by closing its context."""
        if self._context:
            await self._context.close()
            self._context = None
        self._is_connected = False
        await asyncio.sleep(0.1)
        await self._delete_temp_dir(self._context)

    @property
    @override
    def version(self) -> str:
        raise NotImplementedError('Persistent browser does not support version.')

    async def new_page(self, **kwargs: Any) -> Page:
        raise NotImplementedError('Persistent browser does not support new page.')

    @override
    async def new_browser_cdp_session(self) -> CDPSession:
        raise NotImplementedError('Persistent browser does not support new browser CDP session.')

    async def start_tracing(self, **kwargs: Any) -> None:
        raise NotImplementedError('Persistent browser does not support tracing.')

    async def stop_tracing(self, **kwargs: Any) -> bytes:
        raise NotImplementedError('Persistent browser does not support tracing.')
