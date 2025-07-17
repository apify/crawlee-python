from __future__ import annotations

from copy import deepcopy
from http.cookiejar import Cookie, CookieJar
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import NotRequired, Required, TypedDict

from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Iterator


@docs_group('Data structures')
class CookieParam(TypedDict, total=False):
    """Dictionary representation of cookies for `SessionCookies.set` method."""

    name: Required[str]
    """Cookie name."""

    value: Required[str]
    """Cookie value."""

    domain: NotRequired[str]
    """Domain for which the cookie is set."""

    path: NotRequired[str]
    """Path on the specified domain for which the cookie is set."""

    secure: NotRequired[bool]
    """Set the `Secure` flag for the cookie."""

    http_only: NotRequired[bool]
    """Set the `HttpOnly` flag for the cookie."""

    expires: NotRequired[int]
    """Expiration date for the cookie, None for a session cookie."""

    same_site: NotRequired[Literal['Lax', 'None', 'Strict']]
    """Set the `SameSite` attribute for the cookie."""


class PlaywrightCookieParam(TypedDict, total=False):
    """Cookie parameters in Playwright format with camelCase naming."""

    name: NotRequired[str]
    value: NotRequired[str]
    domain: NotRequired[str]
    path: NotRequired[str]
    secure: NotRequired[bool]
    httpOnly: NotRequired[bool]
    expires: NotRequired[float]
    sameSite: NotRequired[Literal['Lax', 'None', 'Strict']]


@docs_group('Data structures')
class SessionCookies:
    """Storage cookies for session with browser-compatible serialization and deserialization."""

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
        """Convert `http.cookiejar.Cookie` to dictionary format.

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

        return PlaywrightCookieParam(**result)

    def _from_playwright(self, cookie_dict: PlaywrightCookieParam) -> CookieParam:
        """Convert Playwright cookie to internal format."""
        result: dict = dict(cookie_dict)

        if 'httpOnly' in result:
            result['http_only'] = result.pop('httpOnly')
        if 'sameSite' in result:
            result['same_site'] = result.pop('sameSite')
        if 'expires' in result:
            expires = int(result['expires'])
            result['expires'] = None if expires == -1 else expires

        return CookieParam(name=result.pop('name', ''), value=result.pop('value', ''), **result)

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
        """Store multiple cookie objects in the session cookie jar.

        Args:
            cookies: A list of cookie objects to store in the jar.
        """
        for cookie in cookies:
            self.store_cookie(cookie)
        self._jar.clear_expired_cookies()

    def set_cookies(self, cookie_dicts: list[CookieParam]) -> None:
        """Create and store cookies from their dictionary representations.

        Args:
            cookie_dicts: List of dictionaries where each dict represents cookie parameters.
        """
        for cookie_dict in cookie_dicts:
            self.set(**cookie_dict)
        self._jar.clear_expired_cookies()

    def get_cookies_as_playwright_format(self) -> list[PlaywrightCookieParam]:
        """Get cookies in playwright format."""
        return [self._to_playwright(cookie) for cookie in self.get_cookies_as_dicts()]

    def set_cookies_from_playwright_format(self, pw_cookies: list[PlaywrightCookieParam]) -> None:
        """Set cookies from playwright format."""
        for pw_cookie in pw_cookies:
            cookie_param = self._from_playwright(pw_cookie)
            self.set(**cookie_param)
        self._jar.clear_expired_cookies()

    def __deepcopy__(self, memo: dict[int, Any] | None) -> SessionCookies:
        # This is necessary because `CookieJar` use `RLock`, which prevents `deepcopy`.
        cookie_dicts = self.get_cookies_as_dicts()
        return self.__class__(deepcopy(cookie_dicts, memo))

    def __len__(self) -> int:
        return len(self._jar)

    def __setitem__(self, name: str, value: str) -> None:
        self.set(name, value)

    def __getitem__(self, name: str) -> str | None:
        for cookie in self._jar:
            if cookie.name == name:
                return cookie.value
        raise KeyError(f"Cookie '{name}' not found")

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

        self_keys = {(cookie.name, cookie.value, cookie.domain, cookie.path) for cookie in self._jar}
        other_keys = {(cookie.name, cookie.value, cookie.domain, cookie.path) for cookie in other.jar}

        return self_keys == other_keys

    def __hash__(self) -> int:
        """Return hash based on the cookies key attributes."""
        cookie_tuples = frozenset((cookie.name, cookie.value, cookie.domain, cookie.path) for cookie in self._jar)
        return hash(cookie_tuples)
