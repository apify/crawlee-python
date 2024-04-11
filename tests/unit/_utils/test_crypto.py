from __future__ import annotations

from crawlee._utils.crypto import compute_short_hash, crypto_random_object_id


def test_crypto_random_object_id_default_length() -> None:
    object_id = crypto_random_object_id()
    assert len(object_id) == 17, 'Default generated object ID should have a length of 17 characters.'


def test_crypto_random_object_id_custom_length() -> None:
    for length in [5, 10, 20, 100]:
        object_id = crypto_random_object_id(length)
        assert len(object_id) == length, f'Generated object ID should have a length of {length} characters.'


def test_crypto_random_object_id_character_set() -> None:
    long_random_object_id = crypto_random_object_id(1000)
    allowed_chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    for char in long_random_object_id:
        assert char in allowed_chars, f"Character '{char}' is not in the expected alphanumeric range."


def test_compute_short_hash_with_known_input() -> None:
    data = b'Hello world!'
    expected_hash = 'c0535e4b'
    assert compute_short_hash(data) == expected_hash, 'The hash does not match the expected output.'


def test_compute_short_hash_with_empty_input() -> None:
    data = b''
    expected_hash = 'e3b0c442'
    assert compute_short_hash(data) == expected_hash, 'The hash for an empty input should follow the expected pattern.'


def test_compute_short_hash_output_length() -> None:
    data = b'some random data'
    assert len(compute_short_hash(data)) == 8, 'The output hash should be 8 characters long.'


def test_compute_short_hash_differentiates_input() -> None:
    data1 = b'input 1'
    data2 = b'input 2'
    assert compute_short_hash(data1) != compute_short_hash(data2), 'Different inputs should produce different hashes.'
