import re

NAME_REGEX = re.compile(r'^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])$')


def validate_storage_name(name: str | None) -> None:
    if name and not NAME_REGEX.match(name):
        raise ValueError(
            f'Invalid storage name "{name}". Name can only contain letters "a" through "z", the digits "0" through'
            '"9", and the hyphen ("-") but only in the middle of the string (e.g. "my-value-1")'
        )
