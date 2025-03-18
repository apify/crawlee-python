from __future__ import annotations

import secrets
from hashlib import sha256


def compute_short_hash(data: bytes, *, length: int = 8) -> str:
    """Compute a hexadecimal SHA-256 hash of the provided data and returns a substring (prefix) of it.

    Args:
        data: The binary data to be hashed.
        length: The length of the hash to be returned.

    Returns:
        A substring (prefix) of the hexadecimal hash of the data.
    """
    hash_object = sha256(data)
    return hash_object.hexdigest()[:length]


def crypto_random_object_id(length: int = 17) -> str:
    """Generate a random object ID."""
    chars = 'abcdefghijklmnopqrstuvwxyzABCEDFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(secrets.choice(chars) for _ in range(length))
