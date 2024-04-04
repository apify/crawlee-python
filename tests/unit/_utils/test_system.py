from __future__ import annotations

from crawlee._utils.digital_size import DigitalSize
from crawlee._utils.system import get_cpu_info, get_memory_info


def test_get_memory_info_returns_valid_values() -> None:
    memory_info = get_memory_info()

    assert DigitalSize(0) < memory_info.total_size < DigitalSize.from_tb(1)
    assert memory_info.current_size < memory_info.total_size


def test_get_cpu_info_returns_valid_values() -> None:
    cpu_info = get_cpu_info()
    assert 0 <= cpu_info.used_ratio <= 1
