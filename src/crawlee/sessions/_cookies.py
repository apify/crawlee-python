from __future__ import annotations

from copy import deepcopy
from http.cookiejar import Cookie
from typing import Any, ClassVar, Literal

from httpx import Cookies
from typing_extensions import override


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

    def _convert_cookie_to_dict(self, cookie: Cookie) -> dict[str, Any]:
        """Convert Cookie object to dictionary format.

        Args:
            cookie: Cookie object to convert.
        """
        cookie_dict: dict[str, Any] = {
            'name': cookie.name,
            'value': cookie.value if cookie.value else '',
            'domain': cookie.domain,
            'path': cookie.path,
            'secure': cookie.secure,
            'http_only': cookie.has_nonstandard_attr('HttpOnly'),
        }

        if cookie.expires:
            cookie_dict['expires'] = cookie.expires

        if cookie.has_nonstandard_attr('SameSite'):
            cookie_dict['same_site'] = cookie.get_nonstandard_attr('SameSite')

        return cookie_dict

    def _normalize_cookie_attributes(self, cookie_dict: dict[str, Any], *, reverse: bool = False) -> dict[str, Any]:
        """Convert cookie attribute keys between internal and browser formats.

        Args:
            cookie_dict: Dictionary with cookie attributes.
            reverse: If True, converts from internal to browser format.
        """
        new_cookie_dict: dict[str, Any] = cookie_dict.copy()

        for key_pair in self._ATTRIBUTE_MAPPING.items():
            new_key, old_key = key_pair
            if reverse:
                old_key, new_key = new_key, old_key

            if old_key in new_cookie_dict:
                new_cookie_dict[new_key] = new_cookie_dict.pop(old_key)

        return new_cookie_dict

    def get_cookies_as_dicts(self) -> list[dict[str, Any]]:
        """Convert cookies to a list format for persistence."""
        return [self._convert_cookie_to_dict(cookie) for cookie in self.jar]

    def get_cookies_as_browser_format(self) -> list[dict[str, Any]]:
        """Get cookies in browser-compatible format."""
        return [self._normalize_cookie_attributes(cookie, reverse=True) for cookie in self.get_cookies_as_dicts()]

    @classmethod
    def from_dict_list(cls, data: list[dict[str, Any]]) -> SessionCookies:
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

    def set_cookies(self, cookie_dicts: list[dict[str, Any]]) -> None:
        """Create and store cookies from their dictionary representations.

        Args:
            cookie_dicts: List of dictionaries where each dict represents cookie parameters.
        """
        for cookie_dict in cookie_dicts:
            normalized_cookie_dict = self._normalize_cookie_attributes(cookie_dict)
            self.set(**normalized_cookie_dict)

    def __deepcopy__(self, memo: dict[int, Any] | None) -> SessionCookies:
        # This is necessary because cookijars use `RLock`, which prevents `deepcopy`.
        cookie_dicts = self.get_cookies_as_dicts()
        return self.__class__.from_dict_list(deepcopy(cookie_dicts, memo))
