from __future__ import annotations

from crawlee import Request


def test_from_url_does_not_mutate_caller_user_data() -> None:
    """Verify `from_url` leaves the caller's `user_data` dict and its nested `__crawlee` block untouched."""
    user_data = {'__crawlee': {'crawlDepth': 2}}
    original_crawlee_data = user_data['__crawlee']

    Request.from_url('https://example.com', user_data=user_data, enqueue_strategy='same-domain')

    assert user_data['__crawlee'] is original_crawlee_data
    assert user_data['__crawlee'] == {'crawlDepth': 2}


def test_from_url_can_reuse_same_user_data_dict() -> None:
    """Verify the same `user_data` dict object can be passed to `from_url` twice without raising."""
    user_data = {'__crawlee': {'crawlDepth': 2}}

    # The first call used to replace the nested `__crawlee` value with a model that rejects item assignment.
    Request.from_url('https://example.com', user_data=user_data, enqueue_strategy='same-domain')
    Request.from_url('https://example.com', user_data=user_data, enqueue_strategy='same-domain')


def test_from_url_omits_crawlee_data_when_empty() -> None:
    """Verify `from_url` does not write a `__crawlee` block when no crawlee-specific options are supplied."""
    request = Request.from_url('https://example.com')

    assert '__crawlee' not in request.model_dump()['user_data']


def test_from_url_keeps_crawlee_data_when_supplied() -> None:
    """Verify `from_url` serializes supplied `max_retries` and `enqueue_strategy` into the `__crawlee` block."""
    request = Request.from_url('https://example.com', max_retries=3, enqueue_strategy='same-hostname')

    assert request.model_dump()['user_data']['__crawlee'] == {
        'maxRetries': 3,
        'enqueueStrategy': 'same-hostname',
    }
    assert request.max_retries == 3
    assert request.enqueue_strategy == 'same-hostname'
