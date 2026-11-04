from __future__ import annotations

import asyncio
import importlib
import sys
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import ValidationError
from yarl import URL

from crawlee.http_clients import CurlImpersonateHttpClient, HttpResponse, HttpxHttpClient, ImpitHttpClient
from crawlee.http_clients._base import _is_cross_origin, _redirect_method
from crawlee.http_clients._httpx import _HttpxResponse
from crawlee.http_clients._impit import _ImpitResponse
from crawlee.proxy_configuration import ProxyInfo
from crawlee.sessions import Session

if TYPE_CHECKING:
    from crawlee.http_clients import HttpClient


@pytest.mark.parametrize(
    ('optional_module_name', 'import_path'),
    [
        pytest.param('curl_cffi', 'crawlee.http_clients._curl_impersonate', id='curl_impersonate'),
        pytest.param('httpx2', 'crawlee.http_clients._httpx', id='httpx'),
    ],
)
def test_import_error_handled(optional_module_name: str, import_path: str) -> None:
    blocked = {
        mod_name: None
        for mod_name in sys.modules
        if mod_name == optional_module_name or mod_name.startswith(f'{optional_module_name}.')
    }
    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name.startswith(import_path):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            importlib.import_module(import_path)


async def test_cookies_of_non_ascii_host_sent_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a host with a non-ASCII name is requested in its IDNA form and gets its cookies back."""
    ascii_url = 'http://xn--bcher-kva.example/'
    response = Mock(spec=HttpResponse, status_code=200, headers={}, url=ascii_url, headers_set_cookie=['sid=1'])
    fetch = AsyncMock(return_value=response)
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_fetch', fetch)
    session = Session()

    await client.send_request('http://b\u00fccher.example/', session=session)
    await client.send_request('http://b\u00fccher.example/', session=session)

    assert fetch.call_args.args[0] == ascii_url
    assert fetch.call_args.kwargs['headers']['cookie'] == 'sid=1'


@pytest.mark.parametrize(
    ('client_class', 'kwarg'),
    [
        pytest.param(ImpitHttpClient, 'cookies', id='impit_cookies'),
        pytest.param(ImpitHttpClient, 'cookie_jar', id='impit_cookie_jar'),
        pytest.param(ImpitHttpClient, 'verify', id='impit_verify'),
        pytest.param(HttpxHttpClient, 'cookies', id='httpx_cookies'),
        pytest.param(HttpxHttpClient, 'auth', id='httpx_auth'),
        pytest.param(HttpxHttpClient, 'params', id='httpx_params'),
        pytest.param(HttpxHttpClient, 'verify', id='httpx_verify'),
        pytest.param(CurlImpersonateHttpClient, 'cookies', id='curl_cookies'),
        pytest.param(CurlImpersonateHttpClient, 'auth', id='curl_auth'),
        pytest.param(CurlImpersonateHttpClient, 'params', id='curl_params'),
        pytest.param(CurlImpersonateHttpClient, 'verify', id='curl_verify'),
    ],
)
def test_ignored_argument_warns(client_class: type[HttpClient], kwarg: str) -> None:
    """Test that an argument of the underlying library that the client ignores raises a warning."""
    with pytest.warns(UserWarning, match=f'`{kwarg}` argument'):
        client_class(**{kwarg: Mock()})


@pytest.mark.parametrize(
    'response_class',
    [
        pytest.param(_ImpitResponse, id='impit'),
        pytest.param(_HttpxResponse, id='httpx'),
    ],
)
async def test_aclose_closes_when_cancelled_while_reading(
    response_class: type[_ImpitResponse | _HttpxResponse],
) -> None:
    """Test that `aclose` closes the response of the library when it is cancelled while reading a small body."""
    library_response = Mock(headers={'content-length': '10'})
    library_response.aread = AsyncMock(side_effect=asyncio.CancelledError)
    library_response.aclose = AsyncMock()
    response = response_class(library_response, deadline=None)

    with pytest.raises(asyncio.CancelledError):
        await response.aclose()

    library_response.aclose.assert_awaited_once()


async def test_send_request_rejects_non_http_scheme(http_client: HttpClient) -> None:
    with pytest.raises(ValidationError):
        await http_client.send_request('gopher://127.0.0.1:6379/_PING')


async def test_timeout_covers_redirect_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that every request of a redirect chain gets what is left of the timeout, until it runs out."""
    url = 'https://example.com/'
    redirect = Mock(spec=HttpResponse, status_code=302, headers={'location': '/next'}, url=url, headers_set_cookie=[])
    fetch = AsyncMock(return_value=redirect)
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_fetch', fetch)
    # The deadline is set at 0, and the requests of the chain start at 0, 4 and 11 seconds.
    monkeypatch.setattr('crawlee.http_clients._base.monotonic', Mock(side_effect=[0, 0, 4, 11]))

    with pytest.raises(TimeoutError):
        await client.send_request(url, timeout=timedelta(seconds=10))

    timeouts = [fetch_call.kwargs['timeout'] for fetch_call in fetch.call_args_list]
    assert timeouts == [timedelta(seconds=10), timedelta(seconds=6)]


async def test_redirect_responses_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that every response of a redirect chain is closed except the returned one."""
    url = 'https://example.com/'
    redirect = Mock(spec=HttpResponse, status_code=302, headers={'location': '/next'}, url=url, headers_set_cookie=[])
    final = Mock(spec=HttpResponse, status_code=200, headers={}, url=f'{url}next', headers_set_cookie=[])
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_fetch', AsyncMock(side_effect=[redirect, final]))

    response = await client.send_request(url)

    assert response is final
    redirect.aclose.assert_awaited_once()
    final.aclose.assert_not_awaited()


async def test_redirect_response_closed_on_invalid_location(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a redirect response whose `Location` can't be parsed is closed before the error propagates."""
    url = 'https://example.com/'
    location = 'http://[invalid'
    redirect = Mock(spec=HttpResponse, status_code=302, headers={'location': location}, url=url, headers_set_cookie=[])
    client = ImpitHttpClient()
    monkeypatch.setattr(client, '_fetch', AsyncMock(return_value=redirect))

    with pytest.raises(ValueError, match='IPv6'):
        await client.send_request(url)

    redirect.aclose.assert_awaited_once()


def test_see_other_keeps_head() -> None:
    """Test that a `303` redirect of a `HEAD` request is followed with `HEAD`."""
    assert _redirect_method(303, 'HEAD') == 'HEAD'


@pytest.mark.parametrize(
    ('url', 'other', 'expected'),
    [
        pytest.param('http://a.com/x', 'http://a.com/x', False, id='same'),
        pytest.param('http://a.com/x', 'https://a.com/x', True, id='different-scheme'),
        pytest.param('http://a.com/x', 'http://b.com/x', True, id='different-host'),
        pytest.param('http://A.com/x', 'http://a.com/y', False, id='different-host-case'),
        pytest.param('http://a.com/x', 'http://a.com:8080/x', True, id='different-port'),
        pytest.param('http://a.com:80/x', 'http://a.com/y', False, id='explicit-default-port'),
        pytest.param('http://a.com/x', 'http://a.com/y', False, id='different-path'),
    ],
)
def test_is_cross_origin(url: str, other: str, *, expected: bool) -> None:
    """Test that a redirect leaves the origin when the scheme, host or port changes."""
    assert _is_cross_origin(URL(url, encoded=True), URL(other, encoded=True)) is expected


@pytest.mark.parametrize(
    'client_class',
    [
        pytest.param(ImpitHttpClient, id='impit'),
        pytest.param(HttpxHttpClient, id='httpx'),
    ],
)
async def test_clients_cached_per_tls_setting(client_class: type[ImpitHttpClient | HttpxHttpClient]) -> None:
    """Test that the requests ignoring TLS errors get a client of their own, while the others share one."""
    client = client_class()

    async with client:
        verified = client._get_client(None, ignore_tls_errors=False)
        unverified = client._get_client(None, ignore_tls_errors=True)

        assert verified is not unverified
        assert client._get_client(None, ignore_tls_errors=False) is verified
        assert client._get_client(None, ignore_tls_errors=True) is unverified


@pytest.mark.parametrize(
    'client_class',
    [
        pytest.param(ImpitHttpClient, id='impit'),
        pytest.param(HttpxHttpClient, id='httpx'),
        pytest.param(CurlImpersonateHttpClient, id='curl'),
    ],
)
@pytest.mark.parametrize(
    ('request_flag', 'proxy_flag', 'expected_flag'),
    [
        pytest.param(False, False, False, id='verified'),
        pytest.param(True, False, True, id='request'),
        pytest.param(False, True, True, id='proxy'),
    ],
)
async def test_ignore_tls_errors_reaches_fetch(
    monkeypatch: pytest.MonkeyPatch,
    client_class: type[HttpClient],
    *,
    request_flag: bool,
    proxy_flag: bool,
    expected_flag: bool,
) -> None:
    """Test that `send_request` asks `_fetch` to ignore TLS errors when the call or its proxy sets the flag."""
    url = 'https://example.com/'
    response = Mock(spec=HttpResponse, status_code=200, headers={}, url=url, headers_set_cookie=[])
    fetch = AsyncMock(return_value=response)
    client = client_class()
    monkeypatch.setattr(client, '_fetch', fetch)
    proxy_info = ProxyInfo(
        url='http://proxy.com:8000',
        scheme='http',
        hostname='proxy.com',
        port=8000,
        ignore_tls_errors=proxy_flag,
    )

    await client.send_request(url, proxy_info=proxy_info, ignore_tls_errors=request_flag)

    assert fetch.call_args.kwargs['ignore_tls_errors'] is expected_flag
