from __future__ import annotations

import pytest
from pydantic import ValidationError

from crawlee import service_locator
from crawlee.configuration import Configuration


@pytest.mark.parametrize('invalid_value', [-0.1, 0.0, 1.5])
def test_configuration_rejects_out_of_range_max_used_cpu_ratio(invalid_value: float) -> None:
    with pytest.raises(ValidationError):
        Configuration(max_used_cpu_ratio=invalid_value)


@pytest.mark.parametrize('invalid_value', [-0.1, 0.0, 1.5])
def test_configuration_rejects_out_of_range_max_used_memory_ratio(invalid_value: float) -> None:
    with pytest.raises(ValidationError):
        Configuration(max_used_memory_ratio=invalid_value)


@pytest.mark.parametrize('valid_value', [0.1, 0.9, 1.0])
def test_configuration_accepts_in_range_max_used_cpu_ratio(valid_value: float) -> None:
    assert Configuration(max_used_cpu_ratio=valid_value).max_used_cpu_ratio == valid_value


@pytest.mark.parametrize('valid_value', [0.1, 0.9, 1.0])
def test_configuration_accepts_in_range_max_used_memory_ratio(valid_value: float) -> None:
    assert Configuration(max_used_memory_ratio=valid_value).max_used_memory_ratio == valid_value


def test_global_configuration_works() -> None:
    assert (
        Configuration.get_global_configuration()
        is Configuration.get_global_configuration()
        is service_locator.get_configuration()
        is service_locator.get_configuration()
    )


def test_global_configuration_works_reversed() -> None:
    assert (
        service_locator.get_configuration()
        is service_locator.get_configuration()
        is Configuration.get_global_configuration()
        is Configuration.get_global_configuration()
    )
