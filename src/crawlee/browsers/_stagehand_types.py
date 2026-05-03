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


@dataclass
class StagehandOptions:
    """Configuration options for Stagehand AI-powered browser automation.

    Controls the execution environment (local or Browserbase cloud), the AI model used
    for page operations, and session-level behaviour such as self-healing and DOM settling.
    """

    env: Literal['LOCAL', 'BROWSERBASE'] = 'LOCAL'
    """Execution environment.
        'LOCAL' - Stagehand runs a local Chromium browser.
        'BROWSERBASE' - Stagehand uses a Browserbase cloud browser session.
    """

    browserbase_api_key: str | None = None
    """API key for authenticating with Browserbase when `env='BROWSERBASE'`. If not provided, read from
    the `BROWSERBASE_API_KEY` environment variable."""

    model_api_key: str | None = None
    """API key for the AI model provider (e.g. OpenAI, Anthropic). Must be provided explicitly - unlike
    Browserbase credentials"""

    project_id: str | None = None
    """Browserbase project ID, required when `env='BROWSERBASE'`. If not provided, read from
    the `BROWSERBASE_PROJECT_ID` environment variable."""

    model: str = 'openai/gpt-4.1-mini'
    """The AI model to use for page operations."""

    verbose: Literal[0, 1, 2] = 0
    """Verbosity level for logging Stagehand session activity.
        0 - quiet
        1 - normal
        2 - debug"""

    self_heal: bool = True
    """When `True`, Stagehand automatically retries failed actions."""

    dom_settle_timeout_ms: float | None = None
    """Maximum time to wait for the DOM to settle before performing an action, in milliseconds."""

    system_prompt: str | None = None
    """Optional system prompt to guide the AI's behavior in sessions."""

    local_ready_timeout_s: float = 10.0
    """Maximum time to wait for the local Stagehand browser to be ready, in seconds."""


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
        """Perform an action on the page using natural language. Argument `page` is automatically set.

        Args:
            **kwargs: Parameters passed to ``AsyncSession.act()``.
                The most common is ``instruction`` — a natural language description
                of the action to perform, e.g. ``instruction='click the login button'``.

        Returns:
            The action result from Stagehand.
        """
        return await self._session.act(page=self, **kwargs)

    async def observe(self, **kwargs: Unpack[SessionObserveParams]) -> SessionObserveResponse:
        """Observe the page and get AI-suggested actions. Argument `page` is automatically set.

        Args:
            **kwargs: Parameters passed to ``AsyncSession.observe()``.
                Optionally pass ``instruction`` to narrow the observation scope.

        Returns:
            Observation result with suggested actions.
        """
        return await self._session.observe(page=self, **kwargs)

    async def extract(self, **kwargs: Unpack[SessionExtractParams]) -> SessionExtractResponse:
        """Extract structured data from the page using natural language. Argument `page` is automatically set.

        Args:
            **kwargs: Parameters passed to ``AsyncSession.extract()``.
                Common parameters: ``instruction`` and ``schema`` (JSON Schema dict).

        Returns:
            Extracted data matching the requested schema.
        """
        return await self._session.extract(page=self, **kwargs)

    async def execute(self, **kwargs: Unpack[SessionExecuteParams]) -> SessionExecuteResponse:
        """Execute arbitrary code on the page via natural language instructions. Argument `page` is automatically set.

        Args:
            **kwargs: Parameters passed to ``AsyncSession.execute()``.
                Common parameters: ``instruction`` describing the code to execute.

        Returns:
            The result of the executed code.
        """
        return await self._session.execute(page=self, **kwargs)
