import pytest
from crawlee._utils.requests import compute_unique_key

@pytest.mark.parametrize(
    ('url', 'method', 'payload', 'headers', 'whitelisted_headers', 'expected_output'),
    [
        ('http://example.com', 'GET', None, {'Accept': 'application/json'}, ['Accept'], 'http://example.com|Accept:application/json'),
        ('http://example.com', 'POST', 'data', {'Authorization': 'Bearer token'}, ['Authorization'], 'POST(a1d0c6e83f027327d8461063f4ac58a6):http://example.com|Authorization:Bearer token'),
        
    ],
    ids=[
        'include_accept_header',
        'include_authorization_header',
    ],
)
def test_compute_unique_key_with_headers(url, method, payload, headers, whitelisted_headers, expected_output) -> None:
    output = compute_unique_key(url, method, payload, headers=headers, whitelisted_headers=whitelisted_headers)
    assert output == expected_output