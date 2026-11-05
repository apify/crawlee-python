"""Fixtures shared by the unit and integration test suites, imported by the conftest of each."""

from __future__ import annotations

import logging
import os
import warnings
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock

import pytest
from curl_cffi import CurlHttpVersion

from crawlee import service_locator
from crawlee._utils.public_suffix import PUBLIC_SUFFIX_LIST_SNAPSHOT
from crawlee.crawlers import BasicCrawler
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_network
from crawlee.http_clients import CurlImpersonateHttpClient, HttpxHttpClient, ImpitHttpClient
from crawlee.statistics import Statistics
from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterator
    from pathlib import Path

    from crawlee.http_clients._base import HttpClient


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


@pytest.fixture(scope='session', autouse=True)
def _offline_public_suffix_list() -> Iterator[None]:
    """Serve the bundled Public Suffix List snapshot instead of downloading it."""
    response = Mock(status_code=200, content=PUBLIC_SUFFIX_LIST_SNAPSHOT.read_bytes())
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr('crawlee._utils.public_suffix.impit', Mock(get=Mock(return_value=response)))
        yield


@pytest.fixture(autouse=True)
def _set_crawler_log_level(pytestconfig: pytest.Config, monkeypatch: pytest.MonkeyPatch) -> None:
    from crawlee import _log_config  # noqa: PLC0415

    loglevel = cast('str | None', pytestconfig.getoption('--log-level'))
    if loglevel is not None:
        monkeypatch.setattr(_log_config, 'get_configured_log_level', lambda: getattr(logging, loglevel.upper()))


@pytest.fixture(scope='session')
def header_network() -> dict:
    return get_available_header_network()


@pytest.fixture
async def key_value_store() -> AsyncGenerator[KeyValueStore, None]:
    kvs = await KeyValueStore.open()
    yield kvs
    await kvs.drop()


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
