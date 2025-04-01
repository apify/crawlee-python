# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Callable, Optional, cast

import pytest
from curl_cffi import CurlHttpVersion
from proxy import Proxy
from uvicorn.config import Config

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_network
from crawlee.http_clients import CurlImpersonateHttpClient, HttpxHttpClient
from crawlee.proxy_configuration import ProxyInfo
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import KeyValueStore, _creation_management
from tests.unit.server import TestServer, app, serve_in_thread

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator
    from pathlib import Path

    from yarl import URL

    from crawlee.http_clients._base import HttpClient


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

        # Reset the flags in the service locator to indicate that no services are explicitly set. This ensures
        # a clean state, as services might have been set during a previous test and not reset properly.
        service_locator._configuration_was_retrieved = False
        service_locator._storage_client_was_retrieved = False
        service_locator._event_manager_was_retrieved = False

        # Reset the services in the service locator.
        service_locator._configuration = None
        service_locator._event_manager = None
        service_locator._storage_client = None

        # Clear creation-related caches to ensure no state is carried over between tests.
        monkeypatch.setattr(_creation_management, '_cache_dataset_by_id', {})
        monkeypatch.setattr(_creation_management, '_cache_dataset_by_name', {})
        monkeypatch.setattr(_creation_management, '_cache_kvs_by_id', {})
        monkeypatch.setattr(_creation_management, '_cache_kvs_by_name', {})
        monkeypatch.setattr(_creation_management, '_cache_rq_by_id', {})
        monkeypatch.setattr(_creation_management, '_cache_rq_by_name', {})

        # Verify that the test environment was set up correctly.
        assert os.environ.get('CRAWLEE_STORAGE_DIR') == str(tmp_path)
        assert service_locator._configuration_was_retrieved is False
        assert service_locator._storage_client_was_retrieved is False
        assert service_locator._event_manager_was_retrieved is False

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


@pytest.fixture(autouse=True)
def _set_crawler_log_level(pytestconfig: pytest.Config, monkeypatch: pytest.MonkeyPatch) -> None:
    from crawlee import _log_config

    loglevel = cast('Optional[str]', pytestconfig.getoption('--log-level'))
    if loglevel is not None:
        monkeypatch.setattr(_log_config, 'get_configured_log_level', lambda: getattr(logging, loglevel.upper()))


@pytest.fixture
async def proxy_info(unused_tcp_port: int) -> ProxyInfo:
    username = 'user'
    password = 'pass'

    return ProxyInfo(
        url=f'http://{username}:{password}@127.0.0.1:{unused_tcp_port}',
        scheme='http',
        hostname='127.0.0.1',
        port=unused_tcp_port,
        username=username,
        password=password,
    )


@pytest.fixture
async def proxy(proxy_info: ProxyInfo) -> AsyncGenerator[ProxyInfo, None]:
    with Proxy(
        [
            '--hostname',
            proxy_info.hostname,
            '--port',
            str(proxy_info.port),
            '--basic-auth',
            f'{proxy_info.username}:{proxy_info.password}',
        ]
    ):
        yield proxy_info


@pytest.fixture
async def disabled_proxy(proxy_info: ProxyInfo) -> AsyncGenerator[ProxyInfo, None]:
    with Proxy(
        [
            '--hostname',
            proxy_info.hostname,
            '--port',
            str(proxy_info.port),
            '--basic-auth',
            f'{proxy_info.username}:{proxy_info.password}',
            '--disable-http-proxy',
        ]
    ):
        yield proxy_info


@pytest.fixture
def memory_storage_client(tmp_path: Path) -> MemoryStorageClient:
    """A fixture for testing the memory storage client and its resource clients."""
    config = Configuration(
        persist_storage=True,
        write_metadata=True,
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )

    return MemoryStorageClient.from_config(config)


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


# It is needed only in some tests, so we use the standard `scope=function`
@pytest.fixture
def redirect_http_server(unused_tcp_port_factory: Callable[[], int]) -> Iterator[TestServer]:
    """Create and start an HTTP test server."""
    config = Config(app=app, lifespan='off', loop='asyncio', port=unused_tcp_port_factory())
    server = TestServer(config=config)
    yield from serve_in_thread(server)


@pytest.fixture
def redirect_server_url(redirect_http_server: TestServer) -> URL:
    """Provide the base URL of the test server."""
    return redirect_http_server.url


@pytest.fixture(
    params=[
        pytest.param('curl', id='curl'),
        pytest.param('httpx', id='httpx'),
    ]
)
async def http_client(request: pytest.FixtureRequest) -> HttpClient:
    if request.param == 'curl':
        return CurlImpersonateHttpClient(http_version=CurlHttpVersion.V1_1)
    return HttpxHttpClient(http2=False)
