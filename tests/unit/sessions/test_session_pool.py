from __future__ import annotations

from typing import AsyncGenerator

import pytest

from crawlee.events import EventManager
from crawlee.sessions import Session, SessionPool

mark = pytest.mark.only()


@pytest.fixture()
async def event_manager() -> AsyncGenerator[EventManager, None]:
    async with EventManager() as em:
        yield em


@pytest.fixture()
async def session_pool(event_manager: EventManager) -> AsyncGenerator[SessionPool, None]:
    async with SessionPool(event_manager) as sp:
        yield sp


@pytest.mark.only()
async def test_get_session_simple(session_pool: SessionPool) -> None:
    session = await session_pool.get_session()

    assert isinstance(session, Session)
    assert session.is_usable is True


#
