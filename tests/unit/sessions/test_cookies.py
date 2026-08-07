from __future__ import annotations

import pytest

from crawlee.sessions._cookies import CookieParam, PlaywrightCookieParam, SessionCookies


@pytest.fixture
def cookie_dict() -> CookieParam:
    return CookieParam(
        {
            'name': 'test_cookie',
            'value': 'test_value',
            'domain': 'example.com',
            'path': '/test',
            'expires': 1735689600,
            'http_only': True,
            'secure': True,
            'same_site': 'Strict',
        }
    )


@pytest.fixture
def session_cookies(cookie_dict: CookieParam) -> SessionCookies:
    session_cookies = SessionCookies()
    session_cookies.set(**cookie_dict)
    return session_cookies


def test_set_basic_cookie() -> None:
    """Test setting a basic cookie with minimal attributes."""
    session_cookies = SessionCookies()
    session_cookies.set('test', 'value')
    cookies = list(session_cookies.jar)

    assert len(cookies) == 1
    cookie = cookies[0]
    assert cookie.name == 'test'
    assert cookie.value == 'value'
    assert cookie.path == '/'
    assert not cookie.secure
    assert not cookie.has_nonstandard_attr('HttpOnly')


def test_set_cookie_with_all_attributes(session_cookies: SessionCookies, cookie_dict: CookieParam) -> None:
    """Test setting a cookie with all available attributes."""
    cookies = list(session_cookies.jar)

    assert len(cookies) == 1
    cookie = cookies[0]

    assert cookie.name == cookie_dict.get('name')
    assert cookie.value == cookie_dict.get('value')
    assert cookie.path == cookie_dict.get('path')
    assert cookie.domain == cookie_dict.get('domain')
    assert cookie.expires == cookie_dict.get('expires')
    assert cookie.has_nonstandard_attr('HttpOnly')
    assert cookie.secure
    assert cookie.get_nonstandard_attr('SameSite') == 'Strict'


def test_convert_cookie_to_dict(session_cookies: SessionCookies, cookie_dict: CookieParam) -> None:
    """Test converting Cookie object to dictionary representation."""
    cookies = list(session_cookies.jar)

    assert len(cookies) == 1
    cookie = cookies[0]

    converted_cookie_dict = session_cookies._convert_cookie_to_dict(cookie)
    assert converted_cookie_dict == cookie_dict


def test_convert_dict_format(session_cookies: SessionCookies) -> None:
    """Test normalizing cookie attributes between internal and browser formats."""
    internal_format = CookieParam({'name': 'test', 'value': 'value', 'http_only': True, 'same_site': 'Lax'})

    # Test internal to browser format
    browser_format = session_cookies._to_playwright(internal_format)
    assert 'httpOnly' in browser_format
    assert 'sameSite' in browser_format
    assert 'http_only' not in browser_format
    assert 'same_site' not in browser_format

    # Test browser to internal format
    browser_format = PlaywrightCookieParam({'name': 'test', 'value': 'value', 'httpOnly': True, 'sameSite': 'Lax'})
    internal_format = session_cookies._from_playwright(browser_format)
    assert 'http_only' in internal_format
    assert 'same_site' in internal_format
    assert 'httpOnly' not in internal_format
    assert 'sameSite' not in internal_format


def test_get_cookies_as_browser_format(session_cookies: SessionCookies, cookie_dict: CookieParam) -> None:
    """Test getting cookies in browser-compatible format."""
    browser_cookies = session_cookies.get_cookies_as_playwright_format()

    assert len(browser_cookies) == 1
    cookie = browser_cookies[0]
    assert 'httpOnly' in cookie
    assert 'sameSite' in cookie
    assert cookie['httpOnly'] == cookie_dict.get('http_only')
    assert cookie['sameSite'] == cookie_dict.get('same_site')


def test_get_cookies_as_dicts(session_cookies: SessionCookies, cookie_dict: CookieParam) -> None:
    """Test get list of dictionary from a SessionCookies."""
    test_session_cookies = session_cookies.get_cookies_as_dicts()

    assert [cookie_dict] == test_session_cookies


def test_convert_cookie_to_dict_preserves_epoch_expiry() -> None:
    """Test that an explicit `expires=0` (the Unix epoch) is not dropped on conversion."""
    session_cookies = SessionCookies()
    session_cookies.set('test', 'value', expires=0)

    cookie_dicts = session_cookies.get_cookies_as_dicts()
    assert len(cookie_dicts) == 1
    assert cookie_dicts[0]['expires'] == 0

    browser_cookies = session_cookies.get_cookies_as_playwright_format()
    assert len(browser_cookies) == 1
    assert browser_cookies[0]['expires'] == 0


def test_store_cookie(session_cookies: SessionCookies) -> None:
    """Test storing a Cookie object directly."""
    test_session_cookies = SessionCookies()
    cookies = list(session_cookies.jar)
    test_session_cookies.store_cookie(cookies[0])

    assert test_session_cookies == session_cookies


def test_store_multidomain_cookies() -> None:
    """Test of storing cookies with the same name for different domains"""
    session_cookies = SessionCookies()
    session_cookies.set(name='a', value='1', domain='test.io')
    session_cookies.set(name='a', value='2', domain='notest.io')
    check_cookies = {
        item.get('domain'): (item['name'], item['value']) for item in session_cookies.get_cookies_as_dicts()
    }

    assert len(check_cookies) == 2

    assert check_cookies['test.io'] == ('a', '1')
    assert check_cookies['notest.io'] == ('a', '2')


def test_extract_cookie_from_header() -> None:
    """Test that cookies are parsed from raw `Set-Cookie` headers with attributes taken from the URL."""
    session_cookies = SessionCookies()
    session_cookies.extract_cookie_from_header(
        'https://example.com/login',
        [
            'sid=abc123; Path=/; Secure; HttpOnly; SameSite=Lax',
            'theme=dark; Domain=example.com; Path=/settings',
        ],
    )
    cookies = {item['name']: item for item in session_cookies.get_cookies_as_dicts()}

    assert cookies['sid']['domain'] == 'example.com'
    assert cookies['sid']['path'] == '/'
    assert cookies['sid']['secure']
    assert cookies['sid']['http_only']
    assert cookies['sid']['same_site'] == 'Lax'

    assert cookies['theme']['domain'] == '.example.com'
    assert cookies['theme']['path'] == '/settings'
    assert not cookies['theme']['secure']


def test_extract_cookie_from_header_uses_url_for_defaults() -> None:
    """Test that the domain of a cookie without the `Domain` attribute comes from the URL of the response."""
    session_cookies = SessionCookies()
    session_cookies.extract_cookie_from_header('https://first.example.com/', ['a=1; Path=/'])
    session_cookies.extract_cookie_from_header('https://second.example.com/', ['b=2; Path=/'])
    domains = {item['name']: item.get('domain') for item in session_cookies.get_cookies_as_dicts()}

    assert domains == {'a': 'first.example.com', 'b': 'second.example.com'}


def test_extract_cookie_from_header_with_empty_headers() -> None:
    """Test that a response without `Set-Cookie` headers leaves the jar untouched."""
    session_cookies = SessionCookies()
    session_cookies.set('existing', 'value', domain='example.com')
    session_cookies.extract_cookie_from_header('https://example.com/', [])

    assert len(session_cookies) == 1


def test_get_cookie_string() -> None:
    """Test that the `Cookie` header contains all cookies matching the URL."""
    session_cookies = SessionCookies()
    session_cookies.set('a', '1', domain='example.com')
    session_cookies.set('b', '2', domain='example.com')

    cookie_string = session_cookies.get_cookie_string('https://example.com/page')

    assert set(cookie_string.split('; ')) == {'a=1', 'b=2'}


def test_get_cookie_string_respects_domain_path_and_secure() -> None:
    """Test that cookies not matching the URL are left out of the `Cookie` header."""
    session_cookies = SessionCookies()
    session_cookies.set('same_domain', '1', domain='example.com')
    session_cookies.set('other_domain', '2', domain='other.com')
    session_cookies.set('other_path', '3', domain='example.com', path='/admin')
    session_cookies.set('secure_only', '4', domain='example.com', secure=True)

    assert session_cookies.get_cookie_string('http://example.com/page') == 'same_domain=1'
    assert set(session_cookies.get_cookie_string('https://example.com/admin').split('; ')) == {
        'same_domain=1',
        'other_path=3',
        'secure_only=4',
    }


def test_get_cookie_string_without_matching_cookies() -> None:
    """Test that an empty string is returned when no cookie matches the URL."""
    session_cookies = SessionCookies()
    session_cookies.set('a', '1', domain='example.com')

    assert session_cookies.get_cookie_string('https://other.com/') == ''


def test_get_cookie_string_round_trip() -> None:
    """Test that cookies extracted from a response are sent back to the same URL."""
    session_cookies = SessionCookies()
    session_cookies.extract_cookie_from_header('https://example.com/login', ['sid=abc123; Path=/'])

    assert session_cookies.get_cookie_string('https://example.com/dashboard') == 'sid=abc123'
