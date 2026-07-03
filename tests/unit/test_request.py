from __future__ import annotations

from crawlee import Request


def test_from_url_does_not_mutate_caller_user_data() -> None:
    user_data = {'__crawlee': {'crawlDepth': 2}}
    original_crawlee_data = user_data['__crawlee']

    Request.from_url('https://example.com', user_data=user_data, enqueue_strategy='same-domain')

    # The caller's dict and its nested `__crawlee` block must be left untouched.
    assert user_data['__crawlee'] is original_crawlee_data
    assert user_data['__crawlee'] == {'crawlDepth': 2}


def test_from_url_can_reuse_same_user_data_dict() -> None:
    user_data = {'__crawlee': {'crawlDepth': 2}}

    # Passing the same dict object twice used to raise because the first call replaced the
    # nested `__crawlee` value with a model that does not support item assignment.
    Request.from_url('https://example.com', user_data=user_data, enqueue_strategy='same-domain')
    Request.from_url('https://example.com', user_data=user_data, enqueue_strategy='same-domain')


def test_from_url_omits_crawlee_data_when_empty() -> None:
    request = Request.from_url('https://example.com')

    # No crawlee-specific options were supplied, so `__crawlee` must not be written to `user_data`.
    assert '__crawlee' not in request.model_dump()['user_data']


def test_from_url_keeps_crawlee_data_when_supplied() -> None:
    request = Request.from_url('https://example.com', max_retries=3, enqueue_strategy='same-hostname')

    assert request.model_dump()['user_data']['__crawlee'] == {
        'maxRetries': 3,
        'enqueueStrategy': 'same-hostname',
    }
    assert request.max_retries == 3
    assert request.enqueue_strategy == 'same-hostname'
