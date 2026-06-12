from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from playwright.async_api import Page

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from stagehand import AsyncSession
    from stagehand.types.session_act_params import SessionActParamsNonStreaming
    from stagehand.types.session_act_response import SessionActResponse
    from stagehand.types.session_execute_params import SessionExecuteParamsNonStreaming
    from stagehand.types.session_execute_response import SessionExecuteResponse
    from stagehand.types.session_extract_params import SessionExtractParamsNonStreaming
    from stagehand.types.session_extract_response import SessionExtractResponse
    from stagehand.types.session_observe_params import SessionObserveParamsNonStreaming
    from stagehand.types.session_observe_response import SessionObserveResponse
    from typing_extensions import Unpack


@dataclass
@docs_group('Browser management')
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

    model: str = 'openai/gpt-5.4-nano'
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


@docs_group('Browser management')
class StagehandPage(Page):
    """A Playwright `Page` enhanced with Stagehand AI methods.

    Wraps a Playwright `Page` and an `AsyncSession`, proxying all standard Playwright methods transparently while adding
    `act()`, `extract()`, `observe()`, and `execute()` AI operations bound to the current page.
    """

    def __init__(self, page: Page, session: AsyncSession) -> None:
        # super().__init__() skipped - Page attribute access delegates to self._page via __getattr__.
        self._page = page
        self._session = session

    def __getattr__(self, name: str) -> Any:
        return getattr(self._page, name)

    @property
    def stagehand_session(self) -> AsyncSession:
        """Provides access to the underlying Stagehand session."""
        return self._session

    async def act(self, **kwargs: Unpack[SessionActParamsNonStreaming]) -> SessionActResponse:
        """Perform an action on the page using natural language. Argument `page` is automatically set.

        Args:
            **kwargs: Parameters passed to `AsyncSession.act()`.
                The most common is `input` — a natural language instruction string or `ActionParam` object, e.g.
                `input='click the login button'`.

        Returns:
            The action result from Stagehand.
        """
        return await self._session.act(page=self._page, **kwargs)

    async def observe(self, **kwargs: Unpack[SessionObserveParamsNonStreaming]) -> SessionObserveResponse:
        """Observe the page and get AI-suggested actions. Argument `page` is automatically set.

        Args:
            **kwargs: Parameters passed to `AsyncSession.observe()`.
                Optionally pass `instruction` to narrow the observation scope.

        Returns:
            Observation result with suggested actions.
        """
        return await self._session.observe(page=self._page, **kwargs)

    async def extract(self, **kwargs: Unpack[SessionExtractParamsNonStreaming]) -> SessionExtractResponse:
        """Extract structured data from the page using natural language. Argument `page` is automatically set.

        Args:
            **kwargs: Parameters passed to `AsyncSession.extract()`.
                Common parameters: `instruction` and `schema` (JSON Schema dict).

        Returns:
            Extracted data matching the requested schema.
        """
        return await self._session.extract(page=self._page, **kwargs)

    async def execute(self, **kwargs: Unpack[SessionExecuteParamsNonStreaming]) -> SessionExecuteResponse:
        """Run an autonomous multi-step AI agent on the page. Argument `page` is automatically set.

        Args:
            **kwargs: Parameters passed to `AsyncSession.execute()`.
                Required parameters:
                - `agent_config` — agent behaviour settings (pass `{}` for defaults).
                Supports keys: `model`, `mode` (`'dom'`/`'hybrid'`/`'cua'`), `system_prompt`, and others.
                - `execute_options` — execution options dict with a required `instruction` key: a natural language
                description of the multi-step task to perform, e.g.
                `execute_options={'instruction': 'find the login form and sign in'}`.

        Returns:
            The result of the agent execution.
        """
        return await self._session.execute(page=self._page, **kwargs)
