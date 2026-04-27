from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from playwright.async_api import Page

if TYPE_CHECKING:
    from stagehand import AsyncSession
    from stagehand.types.session_act_params import SessionActParams
    from stagehand.types.session_act_response import SessionActResponse
    from stagehand.types.session_execute_params import SessionExecuteParams
    from stagehand.types.session_execute_response import SessionExecuteResponse
    from stagehand.types.session_extract_params import SessionExtractParams
    from stagehand.types.session_extract_response import SessionExtractResponse
    from stagehand.types.session_observe_params import SessionObserveParams
    from stagehand.types.session_observe_response import SessionObserveResponse
    from typing_extensions import Unpack


BrowserType = Literal['chromium', 'firefox', 'webkit', 'chrome']


@dataclass
class CrawleePage:
    """Represents a page object within a browser, with additional metadata for tracking and management."""

    id: str
    browser_type: BrowserType
    page: Page


@dataclass
class StagehandOptions:
    env: Literal['LOCAL', 'BROWSERBASE'] = 'LOCAL'
    api_key: str | None = None
    project_id: str | None = None
    model: str = 'openai/gpt-4.1-mini'
    verbose: Literal[0, 1, 2] = 0
    self_heal: bool = True
    dom_settle_timeout_ms: float | None = None
    system_prompt: str | None = None
    local_ready_timeout_s: float = 15.0


class StagehandPage(Page):
    """A Playwright `Page` enhanced with Stagehand AI methods.

    Wraps a Playwright `Page` and an `AsyncSession`, proxying all standard Playwright
    methods transparently while adding `act()`, `extract()`, and `observe()` AI operations
    bound to the current page.
    """

    def __init__(self, page: Page, session: AsyncSession) -> None:
        super().__init__(page._impl_obj)  # noqa: SLF001
        self._session = session

    async def act(self, **kwargs: Unpack[SessionActParams]) -> SessionActResponse:
        """Perform an action on the page using natural language.

        Args:
            **kwargs: Parameters passed to ``AsyncSession.act()``.
                The most common is ``instruction`` — a natural language description
                of the action to perform, e.g. ``instruction='click the login button'``.

        Returns:
            The action result from Stagehand.
        """
        return await self._session.act(page=self, **kwargs)

    async def observe(self, **kwargs: Unpack[SessionObserveParams]) -> SessionObserveResponse:
        """Observe the page and get AI-suggested actions.

        Args:
            **kwargs: Parameters passed to ``AsyncSession.observe()``.
                Optionally pass ``instruction`` to narrow the observation scope.

        Returns:
            Observation result with suggested actions.
        """
        return await self._session.observe(page=self, **kwargs)

    async def extract(self, **kwargs: Unpack[SessionExtractParams]) -> SessionExtractResponse:
        """Extract structured data from the page using natural language.

        Args:
            **kwargs: Parameters passed to ``AsyncSession.extract()``.
                Common parameters: ``instruction`` and ``schema`` (JSON Schema dict).

        Returns:
            Extracted data matching the requested schema.
        """
        return await self._session.extract(page=self, **kwargs)

    async def execute(self, **kwargs: Unpack[SessionExecuteParams]) -> SessionExecuteResponse:
        """Execute arbitrary code on the page via natural language instructions.

        Args:
            **kwargs: Parameters passed to ``AsyncSession.execute()``.
                Common parameters: ``instruction`` describing the code to execute.

        Returns:
            The result of the executed code.
        """
        return await self._session.execute(page=self, **kwargs)
