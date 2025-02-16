from __future__ import annotations

import pytest

from crawlee._utils.byte_size import ByteSize


def test_initializations() -> None:
    assert ByteSize(1024).bytes == 1024
    assert ByteSize.from_kb(1).bytes == 1024
    assert ByteSize.from_mb(1).bytes == 1024**2
    assert ByteSize.from_gb(1).bytes == 1024**3
    assert ByteSize.from_tb(1).bytes == 1024**4

    with pytest.raises(ValueError, match='ByteSize cannot be negative'):
        ByteSize(-1)


def test_conversions() -> None:
    size = ByteSize.from_mb(2)
    assert size.to_kb() == 2 * 1024
    assert size.to_mb() == 2.0
    assert size.to_gb() == 2 / 1024
    assert size.to_tb() == 2 / (1024**2)


def test_string_representation() -> None:
    assert str(ByteSize(512)) == '512 B'
    assert str(ByteSize(2 * 1024)) == '2.00 KB'
    assert str(ByteSize(3 * 1024**2)) == '3.00 MB'
    assert str(ByteSize(4 * 1024**3)) == '4.00 GB'
    assert str(ByteSize(5 * 1024**4)) == '5.00 TB'


def test_comparisons() -> None:
    size1 = ByteSize(1024)
    size2 = ByteSize(512)

    assert size1 > size2
    assert size1 >= size2
    assert size2 < size1
    assert size2 <= size1
    assert size1 == ByteSize(1024)
    assert size1 != size2


def test_additions() -> None:
    # Addition of ByteSize instances
    size1 = ByteSize(1024)
    size2 = ByteSize(2048)
    assert (size1 + size2).bytes == 3072

    # Addition of ByteSize instance and an int
    with pytest.raises(TypeError):
        _ = size1 + 1024

    # Addition of ByteSize instance and an float
    with pytest.raises(TypeError):
        _ = size2 + 123.45


def test_subtractions() -> None:
    # Direct subtraction of ByteSize instances
    size1 = ByteSize(2048)
    size2 = ByteSize(1024)
    assert (size1 - size2).bytes == 1024

    # Subtraction resulting in a negative value raises ValueError
    with pytest.raises(ValueError, match='Resulting ByteSize cannot be negative'):
        _ = size2 - size1

    # Subtraction of ByteSize instance and an int
    with pytest.raises(TypeError):
        _ = size1 - 1024

    # Subtraction of ByteSize instance and an float
    with pytest.raises(TypeError):
        _ = size2 - 123.45


def test_multiplication() -> None:
    # Multiplication of ByteSize by an int
    size = ByteSize(1024)
    result = size * 2
    assert result.bytes == 2048

    # Multiplication of ByteSize by a float
    size_float = ByteSize(1024)
    result_float = size_float * 1.5
    assert result_float.bytes == 1536

    # Test reflected multiplication
    size_reflected = ByteSize(1024)
    reflected_result = 3 * size_reflected
    assert reflected_result.bytes == 3072


def test_divisions() -> None:
    # Division of ByteSize by another ByteSize
    size1 = ByteSize(2048)
    size2 = ByteSize(1024)
    assert (size1 / size2) == 2

    # Division by zero when the divisor is a ByteSize with zero bytes
    with pytest.raises(ZeroDivisionError):
        _ = size1 / ByteSize(0)

    # Division of ByteSize - multiplying by a float
    assert (size1 * 0.5).bytes == 1024
