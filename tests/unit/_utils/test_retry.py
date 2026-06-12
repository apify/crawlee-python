from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

from crawlee._utils.retry import retry_on_error


async def test_success_on_first_attempt() -> None:
    call_mock = AsyncMock()

    @retry_on_error(ValueError)
    async def func() -> bool:
        await call_mock()
        return True

    result = await func()

    assert result is True
    call_mock.assert_called_once()


async def test_retries_and_succeeds() -> None:
    call_mock = AsyncMock()

    @retry_on_error(ValueError, max_attempts=3)
    async def func() -> bool:
        await call_mock()
        if call_mock.call_count < 3:
            raise ValueError('transient')
        return True

    with patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep:
        result = await func()

    assert result is True
    assert call_mock.call_count == 3
    assert mock_sleep.call_count == 2


async def test_reraises_after_max_attempts() -> None:
    @retry_on_error(ValueError, max_attempts=3)
    async def func() -> None:
        raise ValueError('persistent')

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock),
        pytest.raises(ValueError, match='persistent'),
    ):
        await func()


async def test_does_not_retry_on_unspecified_exception() -> None:
    call_mock = AsyncMock()

    @retry_on_error(ValueError, max_attempts=3)
    async def func() -> None:
        await call_mock()
        raise TypeError('not retryable')

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        pytest.raises(TypeError),
    ):
        await func()

    call_mock.assert_called_once()
    assert mock_sleep.call_count == 0


async def test_exponential_backoff_delays() -> None:
    @retry_on_error(ValueError, max_attempts=4, base_delay=timedelta(seconds=1))
    async def func() -> None:
        raise ValueError('test backoff')

    with (
        patch('crawlee._utils.retry._retry_sleep', new_callable=AsyncMock) as mock_sleep,
        pytest.raises(ValueError, match='test backoff'),
    ):
        await func()

    delays = [call.args[0] for call in mock_sleep.call_args_list]
    assert delays == [1.0, 2.0, 4.0]
