# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from collections.abc import Awaitable, Coroutine, Iterator
from typing import TYPE_CHECKING, Any, Callable, Optional, cast
from urllib.parse import parse_qs

import pytest
from proxy import Proxy
from uvicorn.config import Config
from uvicorn.server import Server
from yarl import URL

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_network
from crawlee.proxy_configuration import ProxyInfo
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import KeyValueStore, _creation_management

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path
    from socket import socket


Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Coroutine[None, None, None]]


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


@pytest.fixture
def httpbin() -> URL:
    return cast('URL', URLWrapper(URL(os.environ.get('HTTPBIN_URL', 'https://httpbin.org'))))


async def app(scope: dict[str, Any], receive: Receive, send: Send) -> None:
    """Main ASGI application handler that routes requests to specific handlers.

    Args:
        scope: The ASGI connection scope.
        receive: The ASGI receive function.
        send: The ASGI send function.
    """
    assert scope['type'] == 'http'
    path = scope['path']

    # Route requests to appropriate handlers
    if path.startswith('/start_enqueue'):
        await start_enqueue_endpoint(send)
    elif path.startswith('/asdf'):
        await secondary_index_endpoint(send)
    elif path.startswith('/fdyr'):
        await incapsula_endpoint(send)
    elif path.startswith(('/hjkl', '/qwer', '/uiop')):
        await generic_response_endpoint(send)
    elif path.startswith('/set_cookies'):
        await set_cookies(scope, send)
    elif path.startswith('/cookies'):
        await get_cookies(scope, send)
    elif path.startswith('/status/'):
        await echo_status(scope, send)
    elif path.startswith('/headers'):
        await echo_headers(scope, send)
    elif path.startswith('/post'):
        await post_echo(scope, receive, send)
    else:
        await hello_world(send)


async def get_cookies(scope: dict[str, Any], send: Send) -> None:
    """Handle requests to retrieve cookies sent in the request."""
    headers = scope.get('headers', [])
    cookies = {}
    for header in headers:
        if header[0].decode() == 'cookie':
            cookies_header = header[1].decode()
            for cookie in cookies_header.split(';'):
                name, value = cookie.strip().split('=')
                cookies[name] = value
            break

    await send(
        {
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'content-type', b'application/json'],
            ],
        }
    )
    await send({'type': 'http.response.body', 'body': json.dumps({'cookies': cookies}).encode()})


async def set_cookies(scope: dict[str, Any], send: Send) -> None:
    """Handle requests to set cookies from query parameters and redirect.

    Args:
        scope: The ASGI connection scope.
        send: The ASGI send function.
    """
    query_string = scope.get('query_string', b'').decode()
    query_params = parse_qs(query_string)

    headers = [
        [b'content-type', b'text/plain; charset=utf-8'],
        [b'location', b'/cookies'],  # Redirect header
    ]

    for key, values in query_params.items():
        if values:  # Only add if there's at least one value
            cookie_value = f'{key}={values[0]}; Path=/'
            headers.append([b'set-cookie', cookie_value.encode()])

    await send(
        {
            'type': 'http.response.start',
            'status': 302,  # 302 Found for redirect
            'headers': headers,
        }
    )
    await send({'type': 'http.response.body', 'body': b'Redirecting to get_cookies...'})


async def hello_world(send: Send) -> None:
    """Handle basic requests with a simple HTML response.

    Args:
        send: The ASGI send function.
    """
    await send(
        {
            'type': 'http.response.start',
            'status': 200,
            'headers': [[b'content-type', b'text/html; charset=utf-8']],
        }
    )
    await send(
        {
            'type': 'http.response.body',
            'body': b"""<html>
            <head>
                <title>Hello, world!</title>
            </head>
        </html>""",
        }
    )


async def post_echo(scope: dict[str, Any], receive: Receive, send: Send) -> None:
    """Echo back POST request details similar to httpbin.org/post."""
    # Extract basic request info
    path = scope.get('path', '')
    query_string = scope.get('query_string', b'').decode()

    # Extract headers
    headers = {}
    for name, value in scope.get('headers', []):
        headers[name.decode()] = value.decode()

    # Parse query parameters
    args = {}
    if query_string:
        query_params = parse_qs(query_string)
        for key, values in query_params.items():
            args[key] = values[0] if len(values) == 1 else values

    # Extract cookies
    cookies = {}
    cookie_header: str = headers.get('cookie', '')
    if cookie_header:
        for cookie in cookie_header[0].split(';'):
            name, value = cookie.strip().split('=')
            cookies[name] = value

    # Read the request body
    body = b''
    form = {}
    json_data = None
    more_body = True

    while more_body:
        message = await receive()
        if message['type'] == 'http.request':
            body += message.get('body', b'')
            more_body = message.get('more_body', False)

    # Parse body based on content type
    content_type = headers.get('content-type', '').lower()

    if body and 'application/json' in content_type:
        json_data = json.loads(body.decode())

    if body and 'application/x-www-form-urlencoded' in content_type:
        form_data = parse_qs(body.decode())
        for key, values in form_data.items():
            form[key] = values[0] if len(values) == 1 else values

    body_text = '' if form else body.decode('utf-8', errors='replace')

    # Prepare response
    response = {
        'args': args,
        'data': body_text,
        'files': {},  # Not handling multipart file uploads
        'form': form,
        'headers': headers,
        'json': json_data,
        'origin': headers.get('host', ''),
        'url': f'http://{headers["host"]}{path}',
    }

    response_body = json.dumps(response, indent=2).encode()

    await send(
        {
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'content-type', b'application/json'],
            ],
        }
    )
    await send({'type': 'http.response.body', 'body': response_body})


async def echo_status(scope: dict[str, Any], send: Send) -> None:
    """Echo the status code from the URL path."""
    status_code = int(scope['path'].replace('/status/', ''))
    await send(
        {
            'type': 'http.response.start',
            'status': status_code,
            'headers': [[b'content-type', b'text/plain']],
        }
    )
    await send({'type': 'http.response.body', 'body': b''})


async def echo_headers(scope: dict[str, Any], send: Send) -> None:
    """Echo back the request headers as JSON."""
    headers = {}
    for name, value in scope.get('headers', []):
        headers[name.decode()] = value.decode()

    await send(
        {
            'type': 'http.response.start',
            'status': 200,
            'headers': [[b'content-type', b'application/json']],
        }
    )
    await send({'type': 'http.response.body', 'body': json.dumps(headers, indent=2).encode()})


async def start_enqueue_endpoint(send: Send) -> None:
    """Handle requests for the main page with links.

    Args:
        send: The ASGI send function.
    """
    await send(
        {
            'type': 'http.response.start',
            'status': 200,
            'headers': [[b'content-type', b'text/html']],
        }
    )
    await send(
        {
            'type': 'http.response.body',
            'body': b"""<html>
            <head>
                <title>Hello</title>
            </head>
            <body>
                <a href="/asdf" class="foo">Link 1</a>
                <a href="/hjkl">Link 2</a>
            </body>
        </html>""",
        }
    )


async def secondary_index_endpoint(send: Send) -> None:
    """Handle requests for the secondary page with links.

    Args:
        send: The ASGI send function.
    """
    await send(
        {
            'type': 'http.response.start',
            'status': 200,
            'headers': [[b'content-type', b'text/html']],
        }
    )
    await send(
        {
            'type': 'http.response.body',
            'body': b"""<html>
            <head>
                <title>Hello</title>
            </head>
            <body>
                <a href="/uiop">Link 3</a>
                <a href="/qwer">Link 4</a>
            </body>
        </html>""",
        }
    )


async def incapsula_endpoint(send: Send) -> None:
    """Handle requests for a page with an incapsula iframe."""
    await send(
        {
            'type': 'http.response.start',
            'status': 200,
            'headers': [[b'content-type', b'text/html']],
        }
    )
    await send(
        {
            'type': 'http.response.body',
            'body': b"""<html>
            <head>
                <title>Hello</title>
            </head>
            <body>
                <iframe src=Test_Incapsula_Resource>
                </iframe>
            </body>
        </html>""",
        }
    )


async def generic_response_endpoint(send: Send) -> None:
    """Handle requests with a generic HTML response.

    Args:
        send: The ASGI send function.
    """
    await send(
        {
            'type': 'http.response.start',
            'status': 200,
            'headers': [[b'content-type', b'text/html']],
        }
    )
    await send(
        {
            'type': 'http.response.body',
            'body': b"""<html>
            <head>
                <title>Hello</title>
            </head>
            <body>
                Insightful content
            </body>
        </html>""",
        }
    )


class TestServer(Server):
    """A test HTTP server implementation based on Uvicorn Server."""

    @property
    def url(self) -> URL:
        """Get the base URL of the server.

        Returns:
            A URL instance with the server's base URL.
        """
        protocol = 'https' if self.config.is_ssl else 'http'
        return URL(f'{protocol}://{self.config.host}:{self.config.port}/')

    def install_signal_handlers(self) -> None:
        """Disable the default installation of handlers for signals.

        Override to prevent signal handlers from being installed in non-main threads.
        """

    async def serve(self, sockets: list[socket] | None = None) -> None:
        """Run the server and set up restart capability.

        Args:
            sockets: Optional list of sockets to bind to.
        """
        self.restart_requested = asyncio.Event()

        loop = asyncio.get_event_loop()
        tasks = {
            loop.create_task(super().serve(sockets=sockets)),
            loop.create_task(self.watch_restarts()),
        }
        await asyncio.wait(tasks)

    async def restart(self) -> None:
        """Request server restart and wait for it to complete.

        This method can be called from a different thread than the one the server
        is running on, and from a different async environment.
        """
        self.started = False
        self.restart_requested.set()
        while not self.started:  # noqa: ASYNC110
            await asyncio.sleep(0.2)

    async def watch_restarts(self) -> None:
        """Watch for and handle restart requests."""
        while True:
            if self.should_exit:
                return

            try:
                await asyncio.wait_for(self.restart_requested.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                continue

            self.restart_requested.clear()
            await self.shutdown()
            await self.startup()


def serve_in_thread(server: TestServer) -> Iterator[TestServer]:
    """Run a server in a background thread and yield it."""
    thread = threading.Thread(target=server.run)
    thread.start()
    try:
        while not server.started:
            time.sleep(1e-3)
        yield server
    finally:
        server.should_exit = True
        thread.join()


@pytest.fixture(scope='session')
def http_server(unused_tcp_port_factory: Callable[[], int]) -> Iterator[TestServer]:
    """Create and start an HTTP test server."""
    config = Config(app=app, lifespan='off', loop='asyncio', port=unused_tcp_port_factory())
    server = TestServer(config=config)
    yield from serve_in_thread(server)


@pytest.fixture(scope='session')
def server_url(http_server: TestServer) -> URL:
    """Provide the base URL of the test server."""
    return cast('URL', URLWrapper(http_server.url))
