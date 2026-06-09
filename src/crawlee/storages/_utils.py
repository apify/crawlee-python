import re

NAME_REGEX = re.compile(r'^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])$')


def validate_storage_name(name: str | None) -> None:
    if name and not NAME_REGEX.match(name):
        raise ValueError(
            f'Invalid storage name "{name}". Name can only contain letters "a" through "z", the digits "0" through'
            '"9", and the hyphen ("-") but only in the middle of the string (e.g. "my-value-1")'
        )


def validate_storage_alias(alias: str | None) -> None:
    """Validate a storage alias that is used as an on-disk subdirectory name.

    Unlike storage names, aliases may contain underscores and dots (e.g. the reserved `__default__` alias),
    but must not contain path separators or parent-directory references, so an alias always maps to a single
    directory under the storage directory.
    """
    if alias is None:
        return

    if not alias or '/' in alias or '\\' in alias or '\x00' in alias or alias in {'.', '..'}:
        raise ValueError(
            f'Invalid storage alias "{alias}". Alias must not be empty, contain path separators ("/", "\\") or '
            f'null bytes, or be a directory reference (".", "..").'
        )
