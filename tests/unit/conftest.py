from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis

from tests._fixtures import (  # noqa: F401
    _isolate_test_environment,
    _offline_public_suffix_list,
    _set_crawler_log_level,
    header_network,
    http_client,
    key_value_store,
    prepare_test_env,
    suppress_user_warning,
)


@pytest.fixture
def redis_client() -> FakeAsyncRedis:
    return FakeAsyncRedis()
