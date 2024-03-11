import secrets


def crypto_random_object_id(length: int = 17) -> str:
    """Python reimplementation of cryptoRandomObjectId from `@apify/utilities`."""
    chars = 'abcdefghijklmnopqrstuvwxyzABCEDFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(secrets.choice(chars) for _ in range(length))
