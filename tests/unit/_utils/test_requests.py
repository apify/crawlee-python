from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from crawlee._utils.requests import compute_unique_key, normalize_url, unique_key_to_request_id

if TYPE_CHECKING:
    from crawlee._types import HttpMethod, HttpPayload, HttpHeaders


def test_unique_key_to_request_id_length() -> None:
    unique_key = 'exampleKey123'
    request_id = unique_key_to_request_id(unique_key, request_id_length=15)
    assert len(request_id) == 15, 'Request ID should have the correct length.'


def test_unique_key_to_request_id_consistency() -> None:
    unique_key = 'consistentKey'
    request_id_1 = unique_key_to_request_id(unique_key)
    request_id_2 = unique_key_to_request_id(unique_key)
    assert request_id_1 == request_id_2, 'The same unique key should generate consistent request IDs.'


@pytest.mark.parametrize(
    ('unique_key', 'expected_request_id'),
    [
        ('abc', 'ungWv48BzpBQUDe'),
        ('uniqueKey', 'xiWPs083cree7mH'),
        ('', '47DEQpj8HBSaTIm'),
        ('测试中文', 'lKPdJkdvw8MXEUp'),
        ('test+/=', 'XZRQjhoG0yjfnYD'),
    ],
    ids=[
        'basic_abc',
        'keyword_uniqueKey',
        'empty_string',
        'non_ascii_characters',
        'url_unsafe_characters',
    ],
)
def test_unique_key_to_request_id_known_values(unique_key: str, expected_request_id: str) -> None:
    request_id = unique_key_to_request_id(unique_key)
    assert request_id == expected_request_id, f'Unique key "{unique_key}" should produce the expected request ID.'


@pytest.mark.parametrize(
    ('url', 'expected_output', 'keep_url_fragment'),
    [
        ('https://example.com/?utm_source=test&utm_medium=test&key=value', 'https://example.com?key=value', False),
        (
            'http://example.com/?key=value&another_key=another_value',
            'http://example.com?another_key=another_value&key=value',
            False,
        ),
        ('HTTPS://EXAMPLE.COM/?KEY=VALUE', 'https://example.com?key=value', False),
        ('', '', False),
        ('http://example.com/#fragment', 'http://example.com#fragment', True),
        ('http://example.com/#fragment', 'http://example.com', False),
        ('  https://example.com/  ', 'https://example.com', False),
        ('http://example.com/?b=2&a=1', 'http://example.com?a=1&b=2', False),
    ],
    ids=[
        'remove_utm_params',
        'retain_sort_non_utm_params',
        'convert_scheme_netloc_to_lowercase',
        'handle_empty_url',
        'retain_fragment',
        'remove_fragment',
        'trim_whitespace',
        'sort_query_params',
    ],
)
def test_normalize_url(url: str, expected_output: str, *, keep_url_fragment: bool) -> None:
    output = normalize_url(url, keep_url_fragment=keep_url_fragment)
    assert output == expected_output


@pytest.mark.parametrize(
    ('url', 'method', 'payload', 'keep_url_fragment', 'use_extended_unique_key', 'headers', 'expected_output'),
    [
        ('http://example.com', 'GET', None, False, False, None, 'http://example.com'),
        ('http://example.com', 'POST', None, False, False, None, 'http://example.com'),
        ('http://example.com', 'GET', 'data', False, False, None, 'http://example.com'),
        ('http://example.com', 'GET', 'data', False, True, None, 'GET(3a6eb079):http://example.com'),
        ('http://example.com', 'POST', 'data', False, True, None, 'POST(3a6eb079):http://example.com'),
        ('http://example.com#fragment', 'GET', None, True, False, None, 'http://example.com#fragment'),
        ('http://example.com#fragment', 'GET', None, False, False, None, 'http://example.com'),
        ('http://example.com', 'DELETE', 'test', False, True, None, 'DELETE(9f86d081):http://example.com'),
        ('https://example.com?utm_content=test', 'GET', None, False, False, None, 'https://example.com'),
        ('https://example.com?utm_content=test', 'GET', None, True, False, None, 'https://example.com'),
        ('https://example.com', 'GET', None, False, False, HttpHeaders({"Content-Type": "application/json"}), '{"content-type": "application/json"}:https://example.com'),
        ('https://example.com', 'POST', 'data', False, True, HttpHeaders({"Content-Type": "application/json"}), 'POST(3a6eb079):{"content-type": "application/json"}:https://example.com'),
    ],
    ids=[
        'simple_get',
        'simple_post',
        'get_with_payload',
        'get_with_payload_extended',
        'post_with_payload_extended',
        'get_with_fragment',
        'get_remove_fragment',
        'delete_with_payload_extended',
        'get_remove_utm',
        'get_keep_utm_fragment',
        'get_with_headers',
        'post_with_payload_extended_and_headers',
    ],
)
def test_compute_unique_key(
    url: str,
    method: HttpMethod,
    payload: HttpPayload | None,
    *,
    keep_url_fragment: bool,
    use_extended_unique_key: bool,
    headers: HttpHeaders | None,
    expected_output: str,
) -> None:
    output = compute_unique_key(
        url,
        method,
        payload,
        keep_url_fragment=keep_url_fragment,
        use_extended_unique_key=use_extended_unique_key,
        headers=headers,
    )
    assert output == expected_output
