from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from pydantic import AnyHttpUrl, TypeAdapter
from yarl import URL

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Awaitable

__all__ = ['ProxyConfiguration', 'ProxyInfo']


@dataclass
@docs_group('Other')
class ProxyInfo:
    """Provides information about a proxy connection that is used for requests."""

    url: str
    """The URL of the proxy."""

    scheme: str
    """The scheme of the proxy."""

    hostname: str
    """The hostname of the proxy."""

    port: int
    """The proxy port."""

    username: str = ''
    """The username for the proxy."""

    password: str = ''
    """The password for the proxy."""

    ignore_tls_errors: bool = False
    """Whether the proxy intercepts HTTPS traffic, so that `HttpClient.send_request` and the browser contexts of
    `PlaywrightBrowserController` ignore TLS certificate errors for the requests sent through it. `ProxyConfiguration`
    never sets it, so a subclass that knows its proxy intercepts HTTPS sets it in `new_proxy_info`."""


@docs_group('Configuration')
class ProxyConfiguration:
    """Configures connection to a proxy server with the provided options.

    Proxy servers are used to prevent target websites from blocking your crawlers based on IP address rate limits or
    blacklists. Setting proxy configuration in your crawlers automatically configures them to use the selected proxies
    for all connections. You can get information about the currently used proxy by inspecting the {@apilink ProxyInfo}
    property in your crawler's page function. There, you can inspect the proxy's URL and other attributes.

    If you want to use your own proxies, use the {@apilink ProxyConfigurationOptions.proxyUrls} option. Your list of
    proxy URLs will be rotated by the configuration if this option is provided.
    """

    def __init__(
        self,
        *,
        proxy_urls: list[str | None] | None = None,
        new_url_function: _NewUrlFunction | None = None,
    ) -> None:
        """Initialize a new instance.

        Exactly one of `proxy_urls` or `new_url_function` must be specified.

        Args:
            proxy_urls: A list of URLs of proxies that will be rotated in a round-robin fashion
            new_url_function: A function, sync or async, that returns a new proxy URL, or `None` to use no proxy. This
                provides full control over the proxy selection mechanism.
        """
        self._next_custom_url_index = 0
        self._url_validator = TypeAdapter(AnyHttpUrl)

        # Validation
        if sum(map(bool, (proxy_urls, new_url_function))) != 1:
            raise ValueError('Exactly one of `proxy_urls` and `new_url_function` must be specified (and non-empty).')

        self._proxy_urls = [self._create_url(url) for url in proxy_urls] if proxy_urls else []
        self._new_url_function = new_url_function

    def _create_url(self, url: str | None) -> URL | None:
        """Create URL from input string. None means that intentionally no proxy should be used."""
        if url is None:
            return None

        self._url_validator.validate_python(url)
        return URL(url)

    async def new_proxy_info(self, proxy_info: ProxyInfo | None = None) -> ProxyInfo | None:
        """Pick a `ProxyInfo` by the configured proxy rotation strategy, or return the passed one.

        Args:
            proxy_info: A previously created `ProxyInfo`, such as the one of a restored session. It's returned
                unchanged. A subclass whose proxies depend on the environment returns an equivalent `ProxyInfo` that
                works in the current one.
        """
        if proxy_info is not None:
            return proxy_info

        url = await self._pick_url()

        if url is None:
            return None

        if url.port is None:
            raise ValueError(f'Port is None for URL: {url}')

        if url.host is None:
            raise ValueError(f'Host is None for URL: {url}')

        return ProxyInfo(
            url=str(url),
            scheme=url.scheme,
            hostname=url.host,
            port=url.port,
            username=url.user or '',
            password=url.password or '',
        )

    async def new_url(self) -> str | None:
        """Return a proxy URL string based on the configured proxy rotation strategy."""
        proxy_info = await self.new_proxy_info()
        return proxy_info.url if proxy_info else None

    async def _pick_url(self) -> URL | None:
        if self._new_url_function:
            try:
                result = self._new_url_function()
                if inspect.isawaitable(result):
                    result = await result

                return URL(str(result)) if result is not None else None
            except Exception as e:
                raise ValueError('The provided "new_url_function" did not return a valid URL') from e

        if not self._proxy_urls:
            raise RuntimeError('Invalid state')

        urls = self._proxy_urls
        url = urls[self._next_custom_url_index % len(urls)]
        self._next_custom_url_index += 1
        return url


class _NewUrlFunction(Protocol):
    def __call__(self) -> str | Awaitable[str | None] | None: ...
