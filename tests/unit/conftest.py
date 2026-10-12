from __future__ import annotations

import contextlib
import contextvars
import logging
import os
import socket
import sys
import threading
import warnings
from typing import TYPE_CHECKING, Any, cast

import pytest
from curl_cffi import CurlHttpVersion
from fakeredis import FakeAsyncRedis
from proxy import Proxy
from uvicorn.config import Config

import crawlee.crawlers
from crawlee import service_locator
from crawlee.crawlers import BasicCrawler
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_network
from crawlee.http_clients import CurlImpersonateHttpClient, HttpxHttpClient, ImpitHttpClient
from crawlee.proxy_configuration import ProxyInfo
from crawlee.statistics import Statistics
from crawlee.storages import KeyValueStore
from tests.unit.server import TestServer, app, no_robots_app, serve_in_thread

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator, Iterator
    from pathlib import Path

    from yarl import URL

    from crawlee.http_clients._base import HttpClient


if sys.version_info < (3, 11):
    from backports.asyncio.runner import runner as backports_runner  # ty: ignore[unresolved-import]

    original_copy_context = contextvars.copy_context
    original_patch_object = backports_runner._patch_object
    copy_context_override = threading.local()

    def copy_context() -> contextvars.Context:
        factory = getattr(copy_context_override, 'factory', None)
        return original_copy_context() if factory is None else factory()

    @contextlib.contextmanager
    def patch_object(target: Any, attribute: str, new: Any) -> Generator[None]:
        """Thread-local stand-in for the `_patch_object` helper of `backports.asyncio.runner`.

        On Python 3.10, pytest-asyncio's `Runner.run(coro, context=...)` swaps the module-global
        `contextvars.copy_context` for a lambda returning the fixture's context while it creates the task. If the main
        thread releases the GIL meanwhile (e.g. a GC finalizer closing a socket), the test server's event loop thread
        builds its handles with that same context. When it runs one of them while the main thread steps the fixture
        task, `Context.run` raises "cannot enter context: ... is already entered", the task step is dropped, and the
        fixture never finishes. Keeping the override thread-local confines it to the thread that creates the task.
        """
        if target is not contextvars or attribute != 'copy_context':
            with original_patch_object(target, attribute, new):
                yield
            return
        previous = getattr(copy_context_override, 'factory', None)
        copy_context_override.factory = new
        try:
            yield
        finally:
            copy_context_override.factory = previous

    contextvars.copy_context = copy_context  # ty: ignore[invalid-assignment]
    backports_runner._patch_object = patch_object


@pytest.fixture(autouse=True)
async def suppress_user_warning() -> AsyncGenerator[None, None]:
    """Suppress user warnings during tests.

    Mostly to suppress warnings about the experimental status of the SqlStorageClient.
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        yield


@pytest.fixture
def prepare_test_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Callable[[], None]:
    """Prepare the testing environment by resetting the global state before each test.

    This fixture ensures that the global state of the package is reset to a known baseline before each test runs.
    It also configures a temporary storage directory for test isolation.

    Args:
        monkeypatch: Test utility provided by pytest for patching.
        tmp_path: A unique temporary directory path provided by pytest for test isolation.

    Returns:
        A callable that prepares the test environment.
    """

    def _prepare_test_env() -> None:
        # Disable the browser sandbox by setting the environment variable. This is required for running
        # Playwright tests in the CI environment, where the sandbox is not supported.
        monkeypatch.setenv('CRAWLEE_DISABLE_BROWSER_SANDBOX', 'true')

        # Set the environment variable for the local storage directory to the temporary path.
        monkeypatch.setenv('CRAWLEE_STORAGE_DIR', str(tmp_path))

        # Reset the services in the service locator.
        service_locator._configuration = None
        service_locator._event_manager = None
        service_locator._storage_client = None
        service_locator.storage_instance_manager.clear_cache()

        # Verify that the test environment was set up correctly.
        assert os.environ.get('CRAWLEE_STORAGE_DIR') == str(tmp_path)

        # Reset global class variables to ensure test isolation.
        KeyValueStore._autosaved_values = {}
        Statistics._Statistics__next_id = 0  # ty:ignore[unresolved-attribute] # Mangled attribute
        BasicCrawler._BasicCrawler__next_id = 0  # ty:ignore[unresolved-attribute] # Mangled attribute

    return _prepare_test_env


@pytest.fixture(autouse=True)
def _isolate_test_environment(prepare_test_env: Callable[[], None]) -> None:
    """Isolate the testing environment by resetting global state before and after each test.

    This fixture ensures that each test starts with a clean slate and that any modifications during the test
    do not affect subsequent tests. It runs automatically for all tests.

    Args:
        prepare_test_env: Fixture to prepare the environment before each test.
    """
    prepare_test_env()


@pytest.fixture
def _restore_crawlers_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore the `crawlers` attribute of the `crawlee` package for tests that re-import `crawlee.crawlers`.

    Dropping `crawlee.crawlers` from `sys.modules` and importing it again binds a fresh module object onto the
    `crawlee` package. That object has no private submodule attributes, because the submodules themselves stay
    cached and so are never re-bound. `mock.patch.dict('sys.modules', ...)` restores the module cache but not the
    attribute, which leaves the fresh object reachable through `crawlee.crawlers`. Targets such as
    `crawlee.crawlers._basic._basic_crawler.X` then fail to resolve under `mock.patch` on Python 3.10, which walks
    module attributes; 3.11 and newer go through the module cache instead and are unaffected.
    """
    monkeypatch.setattr(crawlee, 'crawlers', crawlee.crawlers)


@pytest.fixture(autouse=True)
def _set_crawler_log_level(pytestconfig: pytest.Config, monkeypatch: pytest.MonkeyPatch) -> None:
    from crawlee import _log_config  # noqa: PLC0415

    loglevel = cast('str | None', pytestconfig.getoption('--log-level'))
    if loglevel is not None:
        monkeypatch.setattr(_log_config, 'get_configured_log_level', lambda: getattr(logging, loglevel.upper()))


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
def header_network() -> dict:
    return get_available_header_network()


@pytest.fixture
async def key_value_store() -> AsyncGenerator[KeyValueStore, None]:
    kvs = await KeyValueStore.open()
    yield kvs
    await kvs.drop()


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


@pytest.fixture(
    params=[
        pytest.param('httpx', id='httpx'),
        pytest.param('impit', id='impit'),
        pytest.param('curl', id='curl'),
    ]
)
async def http_client(request: pytest.FixtureRequest) -> AsyncGenerator[HttpClient, None]:
    class_client: type[HttpClient]
    kwargs: dict[str, Any]
    if request.param == 'curl':
        class_client = CurlImpersonateHttpClient
        kwargs = {'http_version': CurlHttpVersion.V1_1}
    elif request.param == 'impit':
        class_client = ImpitHttpClient
        kwargs = {'http3': False}
    else:
        class_client = HttpxHttpClient
        kwargs = {'http2': True}
    async with class_client(**kwargs) as client:
        yield client


@pytest.fixture
def redis_client() -> FakeAsyncRedis:
    return FakeAsyncRedis()
