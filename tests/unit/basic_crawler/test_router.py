from __future__ import annotations

import logging
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee.basic_crawler import Router
from crawlee.models import Request
from crawlee.sessions import Session
from crawlee.types import BasicCrawlingContext


class MockContext(BasicCrawlingContext):
    def __init__(self, *, label: str | None) -> None:
        super().__init__(
            request=Request.from_url(url='https://example.com/', user_data={'label': label}),
            session=Session(),
            send_request=AsyncMock(),
            add_requests=AsyncMock(),
            proxy_info=AsyncMock(),
            push_data=AsyncMock(),
            log=logging.getLogger(),
        )


async def test_router_no_handlers() -> None:
    router = Router[MockContext]()

    with pytest.raises(RuntimeError):
        await router(MockContext(label=None))


async def test_router_no_default_handler() -> None:
    router = Router[MockContext]()
    mock_handler = Mock()

    @router.handler('A')
    async def handler_a(_context: MockContext) -> None:
        mock_handler()

    with pytest.raises(RuntimeError):
        await router(MockContext(label='B'))

    mock_handler.assert_not_called()


async def test_router_default_handler_invoked() -> None:
    router = Router[MockContext]()
    mock_default_handler = Mock()
    mock_handler_a = Mock()

    @router.handler('A')
    async def handler_a(_context: MockContext) -> None:
        mock_handler_a()

    @router.default_handler
    async def default_handler(_context: MockContext) -> None:
        mock_default_handler()

    await router(MockContext(label='B'))

    mock_default_handler.assert_called()
    mock_handler_a.assert_not_called()


async def test_router_specific_handler_invoked() -> None:
    router = Router[MockContext]()
    mock_default_handler = Mock()
    mock_handler_a = Mock()
    mock_handler_b = Mock()

    @router.handler('A')
    async def handler_a(_context: MockContext) -> None:
        mock_handler_a()

    @router.handler('B')
    async def handler_b(_context: MockContext) -> None:
        mock_handler_b()

    @router.default_handler
    async def default_handler(_context: MockContext) -> None:
        mock_default_handler()

    await router(MockContext(label='B'))

    mock_default_handler.assert_not_called()
    mock_handler_a.assert_not_called()
    mock_handler_b.assert_called()
