from __future__ import annotations

import socket
from typing import TYPE_CHECKING

import pytest
from filelock import FileLock
from proxy import Proxy
from stagehand._custom.sea_binary import resolve_binary_path
from uvicorn.config import Config

from crawlee.proxy_configuration import ProxyInfo
from tests._fixtures import (  # noqa: F401
    _isolate_test_environment,
    _offline_public_suffix_list,
    _set_crawler_log_level,
    header_network,
    http_client,
    key_value_store,
    prepare_test_env,
    suppress_user_warning,
)
from tests.integration.server import TestServer, app, no_robots_app, serve_in_thread

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from yarl import URL


@pytest.fixture(scope='session')
def stagehand_binary(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Populate Stagehand's per-user binary cache once, serialized across xdist workers."""
    # Stagehand copies its server binary into the cache on first use without a lock, so concurrent first uses race on
    # the copy (`WinError 32` on Windows, `FileNotFoundError` elsewhere).
    with FileLock(tmp_path_factory.getbasetemp().parent / 'stagehand_binary.lock'):
        resolve_binary_path()


def _proxy_info(port: int) -> ProxyInfo:
    """Describe a local proxy listening on `port`, authenticated with fixed throwaway credentials."""
    username = 'user'
    password = 'pass'

    return ProxyInfo(
        url=f'http://{username}:{password}@127.0.0.1:{port}',
        scheme='http',
        hostname='127.0.0.1',
        port=port,
        username=username,
        password=password,
    )


# Session-scoped because a `Proxy` teardown blocks for up to a second on its acceptor's
# `selector.select(timeout=1)`. Sync because a session-scoped async fixture is incompatible with
# `asyncio_default_fixture_loop_scope = "function"`.
@pytest.fixture(scope='session')
def proxy_info(unused_tcp_port_factory: Callable[[], int]) -> ProxyInfo:
    return _proxy_info(unused_tcp_port_factory())


@pytest.fixture(scope='session')
def disabled_proxy_info(unused_tcp_port_factory: Callable[[], int]) -> ProxyInfo:
    """Describe the disabled proxy, which needs a port of its own now that both servers outlive a test."""
    return _proxy_info(unused_tcp_port_factory())


@pytest.fixture(scope='session')
def proxy(proxy_info: ProxyInfo) -> Iterator[ProxyInfo]:
    with Proxy(
        [
            '--hostname',
            proxy_info.hostname,
            '--port',
            str(proxy_info.port),
            '--basic-auth',
            f'{proxy_info.username}:{proxy_info.password}',
            # Without this, `Proxy` spawns one acceptor and one executor process per CPU core, which is
            # unnecessary for a single-test proxy and crashes xdist workers under parallel CI load.
            '--num-workers',
            '1',
            '--num-acceptors',
            '1',
        ]
    ):
        yield proxy_info


@pytest.fixture(scope='session')
def disabled_proxy(disabled_proxy_info: ProxyInfo) -> Iterator[ProxyInfo]:
    with Proxy(
        [
            '--hostname',
            disabled_proxy_info.hostname,
            '--port',
            str(disabled_proxy_info.port),
            '--basic-auth',
            f'{disabled_proxy_info.username}:{disabled_proxy_info.password}',
            '--disable-http-proxy',
            '--num-workers',
            '1',
            '--num-acceptors',
            '1',
        ]
    ):
        yield disabled_proxy_info


@pytest.fixture(scope='session')
def http_server(unused_tcp_port_factory: Callable[[], int]) -> Iterator[TestServer]:
    """Create and start an HTTP test server."""
    config = Config(app=app, lifespan='off', loop='asyncio', port=unused_tcp_port_factory())
    server = TestServer(config=config)
    yield from serve_in_thread(server)


@pytest.fixture(scope='session')
def server_url(http_server: TestServer) -> URL:
    """Provide the base URL of the test server."""
    return http_server.url


@pytest.fixture(scope='session')
def no_robots_http_server(unused_tcp_port_factory: Callable[[], int]) -> Iterator[TestServer]:
    """Create and start an HTTP test server that responds with 404 to robots.txt requests."""
    config = Config(app=no_robots_app, lifespan='off', loop='asyncio', port=unused_tcp_port_factory())
    server = TestServer(config=config)
    yield from serve_in_thread(server)


@pytest.fixture(scope='session')
def no_robots_server_url(no_robots_http_server: TestServer) -> URL:
    """Provide the base URL of the test server that has no robots.txt file."""
    return no_robots_http_server.url


@pytest.fixture(scope='session')
def unreachable_url() -> Iterator[str]:
    """Provide a URL that can never be connected to.

    The port is bound for the whole session but never listened on, so nothing else can take it and every
    connection attempt is refused immediately, without any DNS lookup or external traffic.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        yield f'http://127.0.0.1:{sock.getsockname()[1]}/'


# It is needed only in some tests, so we use the standard `scope=function`
@pytest.fixture
def redirect_http_server(unused_tcp_port_factory: Callable[[], int]) -> Iterator[TestServer]:
    """Create and start an HTTP test server."""
    config = Config(
        app=app,
        lifespan='off',
        loop='asyncio',
        port=unused_tcp_port_factory(),
        limit_max_requests=100,
        timeout_graceful_shutdown=10,
        log_level='error',
        access_log=False,
        ws='websockets-sansio',
    )
    server = TestServer(config=config)
    yield from serve_in_thread(server)


@pytest.fixture
def redirect_server_url(redirect_http_server: TestServer) -> URL:
    """Provide the base URL of the test server."""
    return redirect_http_server.url
