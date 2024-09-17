from crawlee._fingerprint_suite import HeaderGenerator


def test_get_common_headers() -> None:
    header_generator = HeaderGenerator()
    headers = header_generator.get_common_headers()

    assert 'Accept' in headers
    assert 'Accept-Language' in headers
    assert 'User-Agent' in headers
