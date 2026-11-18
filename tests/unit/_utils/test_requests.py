from __future__ import annotations

import pytest

from crawlee._types import HttpHeaders
from crawlee._utils.requests import compute_unique_key, normalize_url


@pytest.mark.parametrize(
    ('url', 'expected_output', 'keep_url_fragment'),
    [
        pytest.param(
            'https://example.com/?utm_source=test&utm_medium=test&key=value',
            'https://example.com/?key=value',
            False,
            id='remove_utm_params',
        ),
        pytest.param(
            'http://example.com/?key=value&another_key=another_value',
            'http://example.com/?another_key=another_value&key=value',
            False,
            id='retain_sort_non_utm_params',
        ),
        pytest.param(
            'HTTPS://EXAMPLE.COM/?KEY=VALUE',
            'https://example.com/?KEY=VALUE',
            False,
            id='lowercase_scheme_and_host_only',
        ),
        pytest.param('', '', False, id='handle_empty_url'),
        pytest.param('http://example.com/#fragment', 'http://example.com/#fragment', True, id='retain_fragment'),
        pytest.param('http://example.com/#fragment', 'http://example.com', False, id='remove_fragment'),
        pytest.param('  https://example.com/  ', 'https://example.com', False, id='trim_whitespace'),
        pytest.param('http://example.com/?b=2&a=1', 'http://example.com/?a=1&b=2', False, id='sort_query_params'),
        pytest.param(
            'https://example.com/Product/ABC', 'https://example.com/Product/ABC', False, id='preserve_path_casing'
        ),
        pytest.param(
            'https://example.com/?token=SeCrEt', 'https://example.com/?token=SeCrEt', False, id='preserve_query_casing'
        ),
        pytest.param(
            'https://example.com/#Fragment', 'https://example.com/#Fragment', True, id='preserve_fragment_casing'
        ),
        pytest.param('https://example.com/a/', 'https://example.com/a', False, id='remove_trailing_slash'),
        pytest.param('https://example.com/a/b', 'https://example.com/a/b', False, id='keep_literal_slash'),
        pytest.param('https://example.com/a%2Fb', 'https://example.com/a%2Fb', False, id='keep_encoded_slash'),
        pytest.param(
            'https://example.com/a%2fb', 'https://example.com/a%2Fb', False, id='uppercase_lowercase_encoded_slash'
        ),
        pytest.param('https://example.com/a%2F', 'https://example.com/a%2F', False, id='keep_encoded_trailing_slash'),
        pytest.param(
            'https://example.com/a%2F/',
            'https://example.com/a%2F',
            False,
            id='remove_literal_slash_after_encoded_slash',
        ),
        pytest.param(
            'https://example.com/a%252Fb', 'https://example.com/a%252Fb', False, id='keep_double_encoded_slash'
        ),
        pytest.param('https://example.com/a+b', 'https://example.com/a+b', False, id='keep_literal_plus'),
        pytest.param('https://example.com/a%2Bb', 'https://example.com/a%2Bb', False, id='keep_encoded_plus'),
        pytest.param(
            'https://example.com/a%2bb', 'https://example.com/a%2Bb', False, id='uppercase_lowercase_encoded_plus'
        ),
        pytest.param('https://example.com/a%3Fb', 'https://example.com/a%3Fb', False, id='keep_encoded_question_mark'),
        pytest.param('https://example.com/%61%7E', 'https://example.com/a~', False, id='decode_unreserved_escapes'),
        pytest.param(
            'https://example.com/a%2F/?z=2&utm_source=test&a=1#fragment',
            'https://example.com/a%2F?a=1&z=2',
            False,
            id='encoded_slash_with_query_remove_fragment',
        ),
        pytest.param(
            'https://example.com/a%2F/?z=2&utm_source=test&a=1#fragment',
            'https://example.com/a%2F?a=1&z=2#fragment',
            True,
            id='encoded_slash_with_query_retain_fragment',
        ),
    ],
)
def test_normalize_url(url: str, expected_output: str, *, keep_url_fragment: bool) -> None:
    """`normalize_url` cleans the URL and lowercases only its scheme and host."""
    output = normalize_url(url, keep_url_fragment=keep_url_fragment)
    assert output == expected_output


def test_compute_unique_key_basic() -> None:
    url = 'https://crawlee.dev'
    uk_get = compute_unique_key(url, method='GET')
    uk_post = compute_unique_key(url, method='POST')
    assert url == uk_get == uk_post


def test_compute_unique_key_distinguishes_path_casing() -> None:
    """URLs that differ only in path casing get distinct unique keys."""
    uk_upper = compute_unique_key('https://example.com/Product/ABC')
    uk_lower = compute_unique_key('https://example.com/product/abc')
    assert uk_upper != uk_lower


def test_compute_unique_key_handles_fragments() -> None:
    url = 'https://crawlee.dev/#fragment'
    uk_with_fragment = compute_unique_key(url, keep_url_fragment=True)
    assert uk_with_fragment == url

    uk_without_fragment = compute_unique_key(url, 'GET', keep_url_fragment=False)
    assert uk_without_fragment == 'https://crawlee.dev'


def test_compute_unique_key_handles_payload() -> None:
    url = 'https://crawlee.dev'
    payload = b'{"key": "value"}'

    # Payload without extended unique key
    uk = compute_unique_key(url, method='POST', payload=payload, use_extended_unique_key=False)
    assert uk == url

    # Extended unique key and payload is None
    uk = compute_unique_key(url, method='POST', payload=None, use_extended_unique_key=True)
    assert uk == 'POST|e3b0c442|e3b0c442|https://crawlee.dev'

    # Extended unique key and payload is bytes
    uk = compute_unique_key(url, method='POST', payload=payload, use_extended_unique_key=True)
    assert uk == 'POST|e3b0c442|9724c1e2|https://crawlee.dev'


def test_compute_unique_key_handles_headers() -> None:
    url = 'https://crawlee.dev'
    headers = HttpHeaders({'Accept': '*/*', 'Content-Type': 'application/json'})
    uk = compute_unique_key(url, headers=headers, use_extended_unique_key=False)
    assert uk == url

    extended_uk_expected = 'GET|4e1a2cf6|e3b0c442|https://crawlee.dev'

    uk = compute_unique_key(url, headers=headers, use_extended_unique_key=True)
    assert uk == extended_uk_expected

    # Accept-Encoding header should not be included.
    headers = HttpHeaders({'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate', 'Content-Type': 'application/json'})
    uk = compute_unique_key(url, headers=headers, use_extended_unique_key=True)
    assert uk == extended_uk_expected


def test_compute_unique_key_complex() -> None:
    url = 'https://crawlee.dev'
    headers = HttpHeaders({'Accept': '*/*', 'Content-Type': 'application/json'})
    payload = b'{"key": "value"}'

    uk = compute_unique_key(
        url,
        method='POST',
        headers=headers,
        payload=payload,
        session_id='test_session',
        use_extended_unique_key=False,
    )
    assert uk == url

    extended_uk = compute_unique_key(
        url,
        method='POST',
        headers=headers,
        payload=payload,
        session_id='test_session',
        use_extended_unique_key=True,
    )
    assert extended_uk == 'POST|4e1a2cf6|9724c1e2|test_session|https://crawlee.dev'


def test_compute_unique_key_post_with_none_payload() -> None:
    url = 'https://crawlee.dev'
    expected_output = 'POST|e3b0c442|e3b0c442|https://crawlee.dev'
    output = compute_unique_key(url, 'POST', payload=None, use_extended_unique_key=True)
    assert output == expected_output


def test_compute_unique_key_with_whitespace_in_headers() -> None:
    url = 'https://crawlee.dev'
    headers = HttpHeaders({'Content-Type': 'application/json'})
    headers_with_whitespaces = HttpHeaders({'Content-Type': ' application/json '})

    expected_output = 'GET|60d83e70|e3b0c442|https://crawlee.dev'
    uk_1 = compute_unique_key(url, headers=headers, use_extended_unique_key=True)
    assert uk_1 == expected_output

    uk_2 = compute_unique_key(url, headers=headers_with_whitespaces, use_extended_unique_key=True)
    assert uk_2 == expected_output


def test_compute_unique_key_keeps_encoded_slash() -> None:
    """An encoded slash in the path gives a different unique key than a literal slash."""
    assert compute_unique_key('https://example.com/a%2Fb') == 'https://example.com/a%2Fb'
    assert compute_unique_key('https://example.com/a/b') == 'https://example.com/a/b'
