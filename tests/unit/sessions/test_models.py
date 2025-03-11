from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crawlee.sessions._cookies import CookieParam
from crawlee.sessions._models import SessionModel, SessionPoolModel

SESSION_CREATED_AT = datetime.now(timezone.utc)


@pytest.fixture
def session_direct() -> SessionModel:
    """Provide a SessionModel instance directly using fixed parameters."""
    return SessionModel(
        id='test_session',
        max_age=timedelta(minutes=30),
        user_data={'user_key': 'user_value'},
        max_error_score=3.0,
        error_score_decrement=0.5,
        created_at=SESSION_CREATED_AT,
        usage_count=0,
        max_usage_count=10,
        error_score=0.0,
        cookies=[CookieParam({'name': 'cookie_key', 'value': 'cookie_value'})],
        blocked_status_codes=[401, 403, 429],
    )


@pytest.fixture
def session_args_camel() -> dict:
    """Provide session parameters as dictionary with camel case keys."""
    return {
        'id': 'test_session',
        'maxAge': '00:30:00',
        'userData': {'user_key': 'user_value'},
        'maxErrorScore': 3.0,
        'errorScoreDecrement': 0.5,
        'createdAt': SESSION_CREATED_AT,
        'usageCount': 0,
        'maxUsageCount': 10,
        'errorScore': 0.0,
        'cookies': [CookieParam({'name': 'cookie_key', 'value': 'cookie_value'})],
        'blockedStatusCodes': [401, 403, 429],
    }


@pytest.fixture
def session_args_snake() -> dict:
    """Provide session parameters as dictionary with snake case keys."""
    return {
        'id': 'test_session',
        'max_age': '00:30:00',
        'user_data': {'user_key': 'user_value'},
        'max_error_score': 3.0,
        'error_score_decrement': 0.5,
        'created_at': SESSION_CREATED_AT,
        'usage_count': 0,
        'max_usage_count': 10,
        'error_score': 0.0,
        'cookies': [CookieParam({'name': 'cookie_key', 'value': 'cookie_value'})],
        'blocked_status_codes': [401, 403, 429],
    }


def test_session_model(
    session_direct: SessionModel,
    session_args_camel: dict,
    session_args_snake: dict,
) -> None:
    """Test equivalence of SessionModel instances created directly and from camelCase, and snake_case kwargs."""
    session_camel = SessionModel(**session_args_camel)
    session_snake = SessionModel(**session_args_snake)

    assert session_direct == session_camel == session_snake
    assert session_direct.id == session_camel.id == session_snake.id == 'test_session'

    # Check that max_age is correctly parsed into a timedelta object
    assert session_direct.max_age == session_camel.max_age == session_snake.max_age == timedelta(minutes=30)


def test_create_session_pool_with_direct_sessions(session_direct: SessionModel) -> None:
    """Test creating a SessionPoolModel with direct session model instances."""
    session_pool = SessionPoolModel(
        persistence_enabled=False,
        persist_state_kvs_name='test_session_pool',
        persist_state_key='crawlee_session_pool_state',
        max_pool_size=3,
        session_count=0,
        usable_session_count=0,
        retired_session_count=0,
        sessions=[session_direct],
    )
    assert session_pool.sessions == [session_direct]


def test_create_session_pool_with_args_sessions(session_args_camel: dict, session_args_snake: dict) -> None:
    """Test creating a SessionPoolModel using sessions initialized from camelCase and snake_case dicts."""
    session_pool_camel = SessionPoolModel(
        persistence_enabled=False,
        persist_state_kvs_name='test_session_pool',
        persist_state_key='crawlee_session_pool_state',
        max_pool_size=3,
        session_count=0,
        usable_session_count=0,
        retired_session_count=0,
        sessions=[session_args_camel, session_args_snake],
    )
    assert session_pool_camel.sessions == [SessionModel(**session_args_camel), SessionModel(**session_args_snake)]
