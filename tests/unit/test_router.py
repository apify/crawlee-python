from __future__ import annotations

import logging
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee import Request
from crawlee._types import BasicCrawlingContext
from crawlee.router import Router
from crawlee.sessions import Session


class MockContext(BasicCrawlingContext):
    def __init__(self, *, label: str | None) -> None:
        super().__init__(
            request=Request.from_url(url='https://example.com/', user_data={'label': label}),
            session=Session(),
            send_request=AsyncMock(),
            add_requests=AsyncMock(),
            proxy_info=AsyncMock(),
            push_data=AsyncMock(),
            use_state=AsyncMock(),
            get_key_value_store=AsyncMock(),
            log=logging.getLogger(),
            register_deferred_cleanup=lambda _: None,
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


async def test_router_handler_not_nullified() -> None:
    router = Router[MockContext]()

    @router.handler('A')
    async def handler_a(_context: MockContext) -> None:
        pass

    assert handler_a is not None


async def test_router_multi_labelled_handler() -> None:
    router = Router[MockContext]()
    mock_handler = Mock()

    @router.handler('A')
    @router.handler('B')
    async def handler(_context: MockContext) -> None:
        mock_handler(_context.request.label)

    await router(MockContext(label='A'))
    mock_handler.assert_called_with('A')
    await router(MockContext(label='B'))
    mock_handler.assert_called_with('B')
    assert mock_handler.call_count == 2


async def test_router_use_middleware() -> None:
    router = Router[MockContext]()
    mock_middleware = Mock()
    mock_default_handler = Mock()

    @router.use
    async def middleware_1(_context: MockContext) -> None:
        mock_middleware(call='middleware_1')

    @router.use
    async def middleware_2(_context: MockContext) -> None:
        mock_middleware(call='middleware_2')

    @router.default_handler
    async def default_handler(_context: MockContext) -> None:
        mock_default_handler()

    await router(MockContext(label=None))

    assert mock_middleware.call_count == 2
    mock_middleware.assert_any_call(call='middleware_1')
    mock_middleware.assert_any_call(call='middleware_2')
    mock_default_handler.assert_called_once()
    # Check order of middleware execution
    assert mock_middleware.call_args_list[0][1] == {'call': 'middleware_1'}
    assert mock_middleware.call_args_list[1][1] == {'call': 'middleware_2'}


async def test_router_use_middleware_with_label() -> None:
    router = Router[MockContext]()
    mock_middleware = Mock()
    mock_handler = Mock()

    @router.use
    async def middleware_1(_context: MockContext) -> None:
        mock_middleware(call='middleware_1')

    @router.use
    async def middleware_2(_context: MockContext) -> None:
        mock_middleware(call='middleware_2')

    @router.handler('A')
    async def handler_a(_context: MockContext) -> None:
        mock_handler(call='handler_a')

    @router.default_handler
    async def default_handler(_context: MockContext) -> None:
        mock_handler(call='default_handler')

    await router(MockContext(label='A'))
    await router(MockContext(label=None))

    assert mock_middleware.call_count == 4
    assert mock_handler.call_count == 2

    assert mock_middleware.call_args_list[0][1] == {'call': 'middleware_1'}
    assert mock_middleware.call_args_list[1][1] == {'call': 'middleware_2'}
    assert mock_handler.call_args_list[0][1] == {'call': 'handler_a'}
    assert mock_middleware.call_args_list[2][1] == {'call': 'middleware_1'}
    assert mock_middleware.call_args_list[3][1] == {'call': 'middleware_2'}
    assert mock_handler.call_args_list[1][1] == {'call': 'default_handler'}


async def test_router_middleware_order_execution() -> None:
    router = Router[MockContext]()
    mock_execution_order = Mock()

    @router.use
    async def middleware_1(_context: MockContext) -> None:
        mock_execution_order(call='middleware_1')

    @router.use
    async def middleware_2(_context: MockContext) -> None:
        mock_execution_order(call='middleware_2')

    @router.default_handler
    async def default_handler(_context: MockContext) -> None:
        mock_execution_order(call='default_handler')

    await router(MockContext(label=None))

    assert mock_execution_order.call_count == 3
    assert mock_execution_order.call_args_list[0][1] == {'call': 'middleware_1'}
    assert mock_execution_order.call_args_list[1][1] == {'call': 'middleware_2'}
    assert mock_execution_order.call_args_list[2][1] == {'call': 'default_handler'}


async def test_router_middleware_exception_interrupts_chain() -> None:
    router = Router[MockContext]()
    mock_execution_order = Mock()

    @router.use
    async def middleware_1(_context: MockContext) -> None:
        mock_execution_order(call='middleware_1')
        raise ValueError('middleware error')

    @router.use
    async def middleware_2(_context: MockContext) -> None:
        mock_execution_order(call='middleware_2')

    @router.default_handler
    async def default_handler(_context: MockContext) -> None:
        mock_execution_order(call='default_handler')

    with pytest.raises(ValueError, match='middleware error'):
        await router(MockContext(label=None))

    assert mock_execution_order.call_count == 1
    assert mock_execution_order.call_args_list[0][1] == {'call': 'middleware_1'}
