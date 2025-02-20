from __future__ import annotations

from copy import deepcopy
from http.cookiejar import Cookie
from typing import Any, ClassVar, Literal, TypedDict, cast

from httpx import Cookies
from typing_extensions import NotRequired, Required, override


class BaseCookieParam(TypedDict, total=False):
    name: Required[str]
    value: Required[str]
    domain: NotRequired[str]
    path: NotRequired[str]
    secure: NotRequired[bool]


class CookieParam(BaseCookieParam, total=False):
    http_only: NotRequired[bool]
    expires: NotRequired[int]
    same_site: NotRequired[Literal['Lax', 'None', 'Strict']]


class PWCookieParam(BaseCookieParam, total=False):
    httpOnly: NotRequired[bool]
    expires: NotRequired[float]
    sameSite: NotRequired[Literal['Lax', 'None', 'Strict']]


class SessionCookies(Cookies):
    """Cookie manager with browser-compatible serialization and deserialization.

    Extends httpx.Cookies with support for browser-specific cookie attributes,
    format conversion and cookie dictionary representations.
    """

    _ATTRIBUTE_MAPPING: ClassVar[dict[str, str]] = {'http_only': 'httpOnly', 'same_site': 'sameSite'}
    """Mapping between internal cookie attribute names and their browser-compatible counterparts."""

    @override
    def set(
        self,
        name: str,
        value: str,
        domain: str = '',
        path: str = '/',
        expires: int | None = None,
        http_only: bool = False,
        secure: bool = False,
        same_site: Literal['Lax', 'None', 'Strict'] | None = None,
    ) -> None:
        """Create and store a cookie with modern browser attributes.

        Args:
            name: Cookie name.
            value: Cookie value.
            domain: Cookie domain.
            path: Cookie path.
            expires: Cookie expiration timestamp.
            http_only: Whether cookie is HTTP-only.
            secure: Whether cookie requires secure context.
            same_site: SameSite cookie attribute value.
        """
        cookie = Cookie(
            version=0,
            name=name,
            value=value,
            port=None,
            port_specified=False,
            domain=domain,
            domain_specified=bool(domain),
            domain_initial_dot=domain.startswith('.'),
            path=path,
            path_specified=bool(path),
            secure=secure,
            expires=expires,
            discard=True,
            comment=None,
            comment_url=None,
            rest={'HttpOnly': ''} if http_only else {},
            rfc2109=False,
        )

        if same_site:
            cookie.set_nonstandard_attr('SameSite', same_site)

        self.jar.set_cookie(cookie)

    def _convert_cookie_to_dict(self, cookie: Cookie) -> CookieParam:
        """Convert Cookie object to dictionary format.

        Args:
            cookie: Cookie object to convert.
        """
        cookie_dict = CookieParam(
            name=cookie.name,
            value=cookie.value if cookie.value else '',
            domain=cookie.domain,
            path=cookie.path,
            secure=cookie.secure,
            http_only=cookie.has_nonstandard_attr('HttpOnly'),
        )

        if cookie.expires:
            cookie_dict['expires'] = cookie.expires

        if (same_site := cookie.get_nonstandard_attr('SameSite')) and same_site in {'Lax', 'None', 'Strict'}:
            cookie_dict['same_site'] = same_site  # type: ignore[typeddict-item]

        return cookie_dict

    def _to_playwright(self, cookie_dict: CookieParam) -> PWCookieParam:
        """Convert internal cookie to Playwright format."""
        result: dict = dict(cookie_dict)

        if 'http_only' in result:
            result['httpOnly'] = result.pop('http_only')
        if 'same_site' in result:
            result['sameSite'] = result.pop('same_site')
        if 'expires' in result:
            result['expires'] = float(result['expires'])

        return cast(PWCookieParam, result)

    def _from_playwright(self, cookie_dict: PWCookieParam) -> CookieParam:
        """Convert Playwright cookie to internal format."""
        result: dict = dict(cookie_dict)

        if 'httpOnly' in result:
            result['http_only'] = result.pop('httpOnly')
        if 'sameSite' in result:
            result['same_site'] = result.pop('sameSite')
        if 'expires' in result:
            result['expires'] = int(result['expires'])

        return cast(CookieParam, result)

    def get_cookies_as_dicts(self) -> list[CookieParam]:
        """Convert cookies to a list format for persistence."""
        return [self._convert_cookie_to_dict(cookie) for cookie in self.jar]

    @classmethod
    def from_dict_list(cls, data: list[CookieParam]) -> SessionCookies:
        """Create a new SessionCookies instance from dictionary representations.

        Args:
            data: List of dictionaries where each dict represents cookie parameters.
        """
        cookies = cls()
        cookies.set_cookies(data)
        return cookies

    def store_cookie(self, cookie: Cookie) -> None:
        """Store a Cookie object in the session cookie jar.

        Args:
            cookie: The Cookie object to store in the jar.
        """
        self.jar.set_cookie(cookie)

    def store_cookies(self, cookies: list[Cookie]) -> None:
        """Store multiple Cookie objects in the session cookie jar.

        Args:
            cookies: A list of Cookie objects to store in the jar.
        """
        for cookie in cookies:
            self.store_cookie(cookie)

    def set_cookies(self, cookie_dicts: list[CookieParam]) -> None:
        """Create and store cookies from their dictionary representations.

        Args:
            cookie_dicts: List of dictionaries where each dict represents cookie parameters.
        """
        for cookie_dict in cookie_dicts:
            self.set(**cookie_dict)

    def get_cookies_as_playwright_format(self) -> list[PWCookieParam]:
        """Get cookies in playwright format."""
        return [self._to_playwright(cookie) for cookie in self.get_cookies_as_dicts()]

    def set_cookies_from_playwright_format(self, pw_cookies: list[PWCookieParam]) -> None:
        """Set cookies from playwright format."""
        for pw_cookie in pw_cookies:
            cookie_param = self._from_playwright(pw_cookie)
            self.set(**cookie_param)

    def __deepcopy__(self, memo: dict[int, Any] | None) -> SessionCookies:
        # This is necessary because cookijars use `RLock`, which prevents `deepcopy`.
        cookie_dicts = self.get_cookies_as_dicts()
        return self.__class__.from_dict_list(deepcopy(cookie_dicts, memo))
