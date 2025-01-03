# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Callable, cast

import pytest
from proxy import Proxy
from yarl import URL

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.proxy_configuration import ProxyInfo
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import _creation_management

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path


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


@pytest.fixture
def httpbin() -> URL:
    class URLWrapper:
        def __init__(self, url: URL) -> None:
            self.url = url

        def __getattr__(self, name: str) -> Any:
            result = getattr(self.url, name)
            return_type = getattr(result, '__annotations__', {}).get('return', None)

            if return_type == 'URL':

                def wrapper(*args: Any, **kwargs: Any) -> URLWrapper:
                    return URLWrapper(result(*args, **kwargs))

                return wrapper

            return result

        def with_path(
            self, path: str, *, keep_query: bool = True, keep_fragment: bool = True, encoded: bool = False
        ) -> URLWrapper:
            return URLWrapper(
                URL.with_path(self.url, path, keep_query=keep_query, keep_fragment=keep_fragment, encoded=encoded)
            )

        def __truediv__(self, other: Any) -> URLWrapper:
            return self.with_path(other)

        def __str__(self) -> str:
            return str(self.url)

    return cast(URL, URLWrapper(URL(os.environ.get('HTTPBIN_URL', 'https://httpbin.org'))))


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
