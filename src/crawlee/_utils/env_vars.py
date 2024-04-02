from __future__ import annotations

import os
from enum import Enum
from typing import Any, Literal, get_args

from crawlee._utils.data_processing import (
    maybe_extract_enum_member_value,
    maybe_parse_bool,
    maybe_parse_datetime,
    maybe_parse_float,
    maybe_parse_int,
)


class CrawleeEnvVars(str, Enum):
    """Enum for the environment variables used by Crawlee."""

    LOCAL_STORAGE_DIR = 'CRAWLEE_LOCAL_STORAGE_DIR'
    PERSIST_STORAGE = 'CRAWLEE_PERSIST_STORAGE'
    PURGE_ON_START = 'CRAWLEE_PURGE_ON_START'


INTEGER_ENV_VARS_TYPE = Literal[None]

INTEGER_ENV_VARS: list[INTEGER_ENV_VARS_TYPE] = list(get_args(INTEGER_ENV_VARS_TYPE))

FLOAT_ENV_VARS_TYPE = Literal[None]

FLOAT_ENV_VARS: list[FLOAT_ENV_VARS_TYPE] = list(get_args(FLOAT_ENV_VARS_TYPE))

BOOL_ENV_VARS_TYPE = Literal[None]

BOOL_ENV_VARS: list[BOOL_ENV_VARS_TYPE] = list(get_args(BOOL_ENV_VARS_TYPE))

DATETIME_ENV_VARS_TYPE = Literal[None]

DATETIME_ENV_VARS: list[DATETIME_ENV_VARS_TYPE] = list(get_args(DATETIME_ENV_VARS_TYPE))

STRING_ENV_VARS_TYPE = Literal[None]

STRING_ENV_VARS: list[STRING_ENV_VARS_TYPE] = list(get_args(STRING_ENV_VARS_TYPE))


def fetch_and_parse_env_var(env_var: Any, default: Any = None) -> Any:
    """Fetches the value of the provided environment variable and parses it according to its type."""
    env_var_name = str(maybe_extract_enum_member_value(env_var))

    val = os.getenv(env_var_name)
    if not val:
        return default

    if env_var in BOOL_ENV_VARS:
        return maybe_parse_bool(val)
    if env_var in FLOAT_ENV_VARS:
        parsed_float = maybe_parse_float(val)
        if parsed_float is None:
            return default
        return parsed_float
    if env_var in INTEGER_ENV_VARS:
        parsed_int = maybe_parse_int(val)
        if parsed_int is None:
            return default
        return parsed_int
    if env_var in DATETIME_ENV_VARS:
        return maybe_parse_datetime(val)
    return val
