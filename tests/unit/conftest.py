from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis

import crawlee.crawlers
from tests._fixtures import (  # noqa: F401
    _isolate_test_environment,
    _offline_public_suffix_list,
    _set_crawler_log_level,
    header_network,
    key_value_store,
    prepare_test_env,
    suppress_user_warning,
)


@pytest.fixture
def _restore_crawlers_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore the `crawlers` attribute of the `crawlee` package for tests that re-import `crawlee.crawlers`.

    Dropping `crawlee.crawlers` from `sys.modules` and importing it again binds a fresh module object onto the
    `crawlee` package. That object has no private submodule attributes, because the submodules themselves stay
    cached and so are never re-bound. `mock.patch.dict('sys.modules', ...)` restores the module cache but not the
    attribute, which leaves the fresh object reachable through `crawlee.crawlers`. Targets such as
    `crawlee.crawlers._basic._basic_crawler.X` then fail to resolve under `mock.patch` on Python 3.10, which walks
    module attributes; 3.11 and newer go through the module cache instead and are unaffected.
    """
    monkeypatch.setattr(crawlee, 'crawlers', crawlee.crawlers)


@pytest.fixture
def redis_client() -> FakeAsyncRedis:
    return FakeAsyncRedis()
