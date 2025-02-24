from __future__ import annotations

from copy import deepcopy
from http.cookiejar import Cookie, CookieJar
from typing import TYPE_CHECKING, Any, Literal, cast

from typing_extensions import NotRequired, Required, TypedDict

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Iterator


class BaseCookieParam(TypedDict, total=False):
    domain: NotRequired[str]
    path: NotRequired[str]
    secure: NotRequired[bool]


@docs_group('Data structures')
class CookieParam(BaseCookieParam, total=False):
    name: Required[str]
    value: Required[str]
    http_only: NotRequired[bool]
    expires: NotRequired[int]
    same_site: NotRequired[Literal['Lax', 'None', 'Strict']]


class PlaywrightCookieParam(BaseCookieParam, total=False):
    name: NotRequired[str]
    value: NotRequired[str]
    httpOnly: NotRequired[bool]
    expires: NotRequired[float]
    sameSite: NotRequired[Literal['Lax', 'None', 'Strict']]


@docs_group('Data structures')
class SessionCookies:
    """Storage Cookies for Session with browser-compatible serialization and deserialization."""

    def __init__(self, cookies: SessionCookies | CookieJar | dict[str, str] | list[CookieParam] | None = None) -> None:
        if isinstance(cookies, CookieJar):
            self._jar = cookies
            return

        self._jar = CookieJar()

        if isinstance(cookies, dict):
            for key, value in cookies.items():
                self.set(key, value)

        elif isinstance(cookies, list):
            for item in cookies:
                self.set(**item)

        elif isinstance(cookies, SessionCookies):
            for cookie in cookies.jar:
                self.jar.set_cookie(cookie)

    @property
    def jar(self) -> CookieJar:
        """The cookie jar instance."""
        return self._jar

    def set(
        self,
        name: str,
        value: str,
        *,
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

    def _to_playwright(self, cookie_dict: CookieParam) -> PlaywrightCookieParam:
        """Convert internal cookie to Playwright format."""
        result: dict = dict(cookie_dict)

        if 'http_only' in result:
            result['httpOnly'] = result.pop('http_only')
        if 'same_site' in result:
            result['sameSite'] = result.pop('same_site')
        if 'expires' in result:
            result['expires'] = float(result['expires'])

        return cast(PlaywrightCookieParam, result)

    def _from_playwright(self, cookie_dict: PlaywrightCookieParam) -> CookieParam:
        """Convert Playwright cookie to internal format."""
        result: dict = dict(cookie_dict)

        if 'httpOnly' in result:
            result['http_only'] = result.pop('httpOnly')
        if 'sameSite' in result:
            result['same_site'] = result.pop('sameSite')
        if 'expires' in result:
            result['expires'] = int(result['expires'])
        if 'name' not in result:
            result['name'] = ''
        if 'value' not in result:
            result['value'] = ''

        return cast(CookieParam, result)

    def get_cookies_as_dicts(self) -> list[CookieParam]:
        """Convert cookies to a list with `CookieParam` dicts."""
        return [self._convert_cookie_to_dict(cookie) for cookie in self.jar]

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

    def get_cookies_as_playwright_format(self) -> list[PlaywrightCookieParam]:
        """Get cookies in playwright format."""
        return [self._to_playwright(cookie) for cookie in self.get_cookies_as_dicts()]

    def set_cookies_from_playwright_format(self, pw_cookies: list[PlaywrightCookieParam]) -> None:
        """Set cookies from playwright format."""
        for pw_cookie in pw_cookies:
            cookie_param = self._from_playwright(pw_cookie)
            self.set(**cookie_param)

    def __deepcopy__(self, memo: dict[int, Any] | None) -> SessionCookies:
        # This is necessary because `CookieJar` use `RLock`, which prevents `deepcopy`.
        cookie_dicts = self.get_cookies_as_dicts()
        return self.__class__(deepcopy(cookie_dicts, memo))

    def __len__(self) -> int:
        return len(self._jar)

    def __set__(self, name: str, value: str) -> None:
        self.set(name, value)

    def __iter__(self) -> Iterator[CookieParam]:
        return (self._convert_cookie_to_dict(cookie) for cookie in self._jar)

    def __repr__(self) -> str:
        cookies_str: str = ', '.join(
            [f'<Cookie {cookie.name}={cookie.value} for {cookie.domain}{cookie.path}>' for cookie in self._jar]
        )
        return f'<SessionCookies[{cookies_str}]>'

    def __bool__(self) -> bool:
        for _ in self._jar:
            return True
        return False

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SessionCookies):
            return NotImplemented

        if len(self) != len(other):
            return False

        self_cookies = {
            (cookie.name, cookie.value, cookie.domain, cookie.path): self._convert_cookie_to_dict(cookie)
            for cookie in self._jar
        }

        for cookie in other.jar:
            key = (cookie.name, cookie.value, cookie.domain, cookie.path)
            if key not in self_cookies:
                return False

            if self_cookies[key] != self._convert_cookie_to_dict(cookie):
                return False

        return True
