from __future__ import annotations

from crawlee import Request
from crawlee._request import UserData


def test_label_property_with_normal_request() -> None:
    """Test that label property works correctly with normal request creation."""
    request = Request.from_url('https://example.com', label='test-label')
    assert request.label == 'test-label'


def test_label_property_without_label() -> None:
    """Test that label property returns None when no label is set."""
    request = Request.from_url('https://example.com')
    assert request.label is None


def test_crawlee_data_property_with_normal_request() -> None:
    """Test that crawlee_data property works correctly with normal request creation."""
    request = Request.from_url('https://example.com', max_retries=5)
    assert request.crawlee_data.max_retries == 5


def test_label_works_when_user_data_is_plain_dict() -> None:
    """Test that label property works even when user_data is a plain dict.

    This can happen when model_construct() is used to bypass Pydantic validation.
    Without the fix, this would raise: AttributeError: 'dict' object has no attribute 'label'
    """
    # model_construct() bypasses validation, so user_data remains a plain dict
    request = Request.model_construct(
        unique_key='test-key',
        url='https://example.com',
        method='GET',
        user_data={'label': 'test-label'},
    )

    # Verify user_data is actually a plain dict (not UserData)
    assert isinstance(request.user_data, dict)
    assert not isinstance(request.user_data, UserData)

    # This would fail with old code: AttributeError: 'dict' object has no attribute 'label'
    # With the fix, it works correctly
    assert request.label == 'test-label'
