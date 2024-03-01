from __future__ import annotations

import pytest

from crawlee._utils.system import get_cpu_info, get_memory_info

pytestmark = pytest.mark.asyncio()


async def test_get_memory_info_returns_valid_values() -> None:
    memory_info = get_memory_info()

    assert 0 < memory_info.used_bytes < memory_info.total_bytes
    assert memory_info.available_bytes + memory_info.used_bytes < memory_info.total_bytes
    assert memory_info.current_process_bytes + memory_info.child_processes_bytes < memory_info.used_bytes


async def test_get_cpu_info_returns_valid_values() -> None:
    cpu_info = await get_cpu_info()
    assert 0 <= cpu_info.used_ratio <= 1
