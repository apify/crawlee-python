from __future__ import annotations

import shutil
import tempfile
from functools import partial
from logging import getLogger
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from playwright.async_api import BrowserContext, BrowserType

logger = getLogger(__name__)


class PlaywrightPersistentBrowser:
    """Wrapper for Browser that uses persistent context under the hood."""

    def __init__(
        self,
        browser_type: BrowserType,
        user_data_dir: str | Path | None,
        browser_launch_options: dict[str, Any],
    ) -> None:
        self._browser_type = browser_type
        self._browser_launch_options = browser_launch_options
        self._user_data_dir = user_data_dir

        self._context: BrowserContext | None = None
        self._is_connected = True

    @property
    def browser_type(self) -> BrowserType:
        return self._browser_type

    def is_connected(self) -> bool:
        return self._is_connected

    async def new_context(self, **context_options: Any) -> BrowserContext:
        """Creates persistent context instead of regular one. Merges launch options with context options."""
        if self._context:
            raise RuntimeError('Persistent browser can have only one context')

        launch_options = self._browser_launch_options | context_options

        if self._user_data_dir:
            user_data_dir = self._user_data_dir
            temp_dir = False
        else:
            user_data_dir = tempfile.mkdtemp(prefix='apify-playwright-firefox-taac-')
            temp_dir = True

        self._context = await self._browser_type.launch_persistent_context(
            user_data_dir=user_data_dir, **launch_options
        )

        if temp_dir:
            self._context.on('close', partial(shutil.rmtree, user_data_dir))

        return self._context

    async def close(self) -> None:
        """Close browser by closing its context."""
        if self._context:
            await self._context.close()
            self._context = None
        self._is_connected = False
