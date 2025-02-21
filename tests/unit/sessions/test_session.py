from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crawlee.sessions._cookies import SessionCookies
from crawlee.sessions._session import Session


@pytest.fixture
def session() -> Session:
    return Session(
        id='test_session',
        max_age=timedelta(minutes=30),
        user_data={'user_key': 'user_value'},
        max_error_score=3.0,
        error_score_decrement=0.5,
        created_at=datetime.now(timezone.utc),
        usage_count=0,
        max_usage_count=10,
        error_score=0.0,
        cookies={'cookie_key': 'cookie_value'},
        blocked_status_codes=[401, 403, 429],
    )


def test_session_init(session: Session) -> None:
    """Verify that the session initializes correctly with the expected properties."""
    assert session.id == 'test_session'
    assert session.user_data == {'user_key': 'user_value'}
    assert session.cookies == SessionCookies({'cookie_key': 'cookie_value'})
    assert session.expires_at >= datetime.now(timezone.utc)
    assert not session.is_blocked
    assert not session.is_expired
    assert not session.is_max_usage_count_reached
    assert session.is_usable


def test_session_get_state(session: Session) -> None:
    """Check if the session state is correctly retrievable in both dict and model forms."""
    session_state_dict = session.get_state(as_dict=True)
    assert session_state_dict['id'] == 'test_session'

    session_state_model = session.get_state(as_dict=False)
    assert session_state_model.id == 'test_session'

    session_2 = Session.from_model(session_state_model)
    assert session_2.id == 'test_session'


def test_mark_good(session: Session) -> None:
    """Test the mark_good method increases usage count and potentially decreases error score."""
    initial_usage_count = session.usage_count
    session.mark_good()
    assert session.usage_count == initial_usage_count + 1
    assert session.error_score == 0


def test_mark_bad(session: Session) -> None:
    """Test the mark_bad method affects the session's error score and usage."""
    initial_error_score = session.error_score
    session.mark_bad()
    assert session.error_score == initial_error_score + 1


def test_multiple_marks(session: Session) -> None:
    """Test the mark_good and mark_bad methods in sequence."""
    initial_usage_count = session.usage_count
    session.mark_bad()
    session.mark_bad()
    assert session.error_score == initial_usage_count + 2
    session.mark_good()
    session.mark_good()
    assert session.error_score == initial_usage_count + 1
    session.mark_bad()
    session.mark_bad()
    session.mark_good()
    assert session.is_blocked
    assert not session.is_usable


def test_retire_method(session: Session) -> None:
    """Test that retire method properly sets the session as unusable."""
    session.retire()
    assert not session.is_usable
    assert session.error_score == 3.0


def test_retire_on_blocked_status_code(session: Session) -> None:
    """Test retiring the session based on specific HTTP status codes."""
    status_code = 403
    result = session.is_blocked_status_code(status_code=status_code)
    assert result is True


def test_not_retire_on_not_block_status_code(session: Session) -> None:
    """Test that the session is not retired on a non-blocked status code."""
    status_code = 200
    result = session.is_blocked_status_code(status_code=status_code)
    assert result is False


def test_session_expiration() -> None:
    """Test the expiration logic of the session."""
    session = Session(created_at=datetime.now(timezone.utc) - timedelta(hours=1))
    assert session.is_expired
