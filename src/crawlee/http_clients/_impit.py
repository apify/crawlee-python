from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any, Optional

from impit import AsyncClient, Response
from typing_extensions import override

from crawlee._types import HttpHeaders
from crawlee._utils.docs import docs_group
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.http_clients import HttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from crawlee import Request
    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics

logger = getLogger(__name__)


class _ImpitResponse:
    """Adapter class for `impit.Response` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: Response) -> None:
        self._response = response

    @property
    def http_version(self) -> str:
        return str(self._response.http_version)

    @property
    def status_code(self) -> int:
        return int(self._response.status_code)

    @property
    def headers(self) -> HttpHeaders:
        return HttpHeaders(dict(self._response.headers))

    def read(self) -> bytes:
        return self._response.content


@docs_group('Classes')
class ImpitHttpClient(HttpClient):
    """HTTP client based on the `impit` library.

    This client uses the `impit` library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.

    See the `HttpClient` class for more common information about HTTP clients.

    ### Usage

    ```python
    from crawlee.crawlers import HttpCrawler  # or any other HTTP client-based crawler
    from crawlee.http_clients import ImpitHttpClient

    http_client = ImpitHttpClient()
    crawler = HttpCrawler(http_client=http_client)
    ```
    """

    _DEFAULT_HEADER_GENERATOR = HeaderGenerator()

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        http3: bool = True,
        verify: bool = True,
        **async_client_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            http3: Whether to enable HTTP/3 support.
            verify: SSL certificates used to verify the identity of requested hosts.
            header_generator: Header generator instance to use for generating common headers.
            async_client_kwargs: Additional keyword arguments for `httpx.AsyncClient`.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
        )
        self._http3 = http3
        self._verify = verify

        self._async_client_kwargs = async_client_kwargs

        self._client_by_proxy_url = dict[Optional[str], AsyncClient]()

    @override
    async def crawl(
        self,
        request: Request,
        *,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        statistics: Statistics | None = None,
    ) -> HttpCrawlingResult:
        client = self._get_client(proxy_info.url if proxy_info else None)

        response = await client.request(
            url=request.url,
            method=request.method,
            content=request.payload,
            headers=dict(request.headers) if request.headers else None,
        )

        if statistics:
            statistics.register_status_code(response.status_code)

        request.loaded_url = str(response.url)

        return HttpCrawlingResult(
            http_response=_ImpitResponse(response),
        )

    @override
    async def send_request(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> HttpResponse:
        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        client = self._get_client(proxy_info.url if proxy_info else None)

        response = await client.request(
            url=url,
            method=method,
            headers=dict(headers) if headers else None,
            content=payload,
        )

        return _ImpitResponse(response)

    def _get_client(self, proxy_url: str | None) -> AsyncClient:
        """Retrieve or create an HTTP client for the given proxy URL.

        If a client for the specified proxy URL does not exist, create and store a new one.
        """
        if proxy_url not in self._client_by_proxy_url:
            # Prepare a default kwargs for the new client.
            kwargs: dict[str, Any] = {
                'proxy': proxy_url,
                'http3': self._http3,
                'verify': self._verify,
                'follow_redirects': True,
            }

            # Update the default kwargs with any additional user-provided kwargs.
            kwargs.update(self._async_client_kwargs)

            client = AsyncClient(**kwargs)
            self._client_by_proxy_url[proxy_url] = client

        return self._client_by_proxy_url[proxy_url]
