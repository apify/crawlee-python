from __future__ import annotations

from crawlee._utils.system import get_cpu_info, get_memory_info


def test_get_memory_info_returns_valid_values() -> None:
    memory_info = get_memory_info()

    assert 0 < memory_info.total_bytes < 1024**4  # 1TB
    assert memory_info.current_bytes < memory_info.total_bytes


def test_get_cpu_info_returns_valid_values() -> None:
    cpu_info = get_cpu_info()
    assert 0 <= cpu_info.used_ratio <= 1
