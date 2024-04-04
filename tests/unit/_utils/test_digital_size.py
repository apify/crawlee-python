import pytest

from crawlee._utils.digital_size import DigitalSize


def test_initializations() -> None:
    assert DigitalSize(1024).bytes_ == 1024
    assert DigitalSize.from_kb(1).bytes_ == 1024
    assert DigitalSize.from_mb(1).bytes_ == 1024**2
    assert DigitalSize.from_gb(1).bytes_ == 1024**3
    assert DigitalSize.from_tb(1).bytes_ == 1024**4

    with pytest.raises(ValueError, match='DigitalSize cannot be negative'):
        DigitalSize(-1)


def test_conversions() -> None:
    size = DigitalSize.from_mb(2)
    assert size.to_kb() == 2 * 1024
    assert size.to_mb() == 2.0
    assert size.to_gb() == 2 / 1024
    assert size.to_tb() == 2 / (1024**2)


def test_string_representation() -> None:
    assert str(DigitalSize(512)) == '512 B'
    assert str(DigitalSize(2 * 1024)) == '2.00 KB'
    assert str(DigitalSize(3 * 1024**2)) == '3.00 MB'
    assert str(DigitalSize(4 * 1024**3)) == '4.00 GB'
    assert str(DigitalSize(5 * 1024**4)) == '5.00 TB'


def test_comparisons() -> None:
    size1 = DigitalSize(1024)
    size2 = DigitalSize(512)

    assert size1 > size2
    assert size1 >= size2
    assert size2 < size1
    assert size2 <= size1
    assert size1 == DigitalSize(1024)
    assert size1 != size2


def test_additions() -> None:
    # Direct addition of DigitalSize instances
    size1 = DigitalSize(1024)
    size2 = DigitalSize(2048)
    assert (size1 + size2).bytes_ == 3072

    # Addition of DigitalSize instance and an int
    assert (size1 + 1024).bytes_ == 2048

    # Addition of DigitalSize instance and an float
    assert (size2 + 1024.0).bytes_ == 3072

    # Test reflected addition
    assert (1024 + size1).bytes_ == 2048


def test_subtractions() -> None:
    # Direct subtraction of DigitalSize instances
    size1 = DigitalSize(2048)
    size2 = DigitalSize(1024)
    assert (size1 - size2).bytes_ == 1024

    # Subtraction resulting in a negative value raises ValueError
    with pytest.raises(ValueError, match='Resulting DigitalSize cannot be negative'):
        _ = size2 - size1

    # Subtraction of an int from a DigitalSize
    size3 = DigitalSize(3072)
    assert (size3 - 1024).bytes_ == 2048

    # Subtraction of a float from a DigitalSize
    assert (size3 - 1024.0).bytes_ == 2048

    # Test reflected subtraction
    assert (4096 - size3).bytes_ == 1024

    # Reflected subtraction resulting in a negative value raises ValueError
    with pytest.raises(ValueError, match='Resulting DigitalSize cannot be negative'):
        _ = 512 - size3


def test_multiplication() -> None:
    # Multiplication of DigitalSize by an int
    size = DigitalSize(1024)
    result = size * 2
    assert result.bytes_ == 2048

    # Multiplication of DigitalSize by a float
    size_float = DigitalSize(1024)
    result_float = size_float * 1.5
    assert result_float.bytes_ == 1536

    # Test reflected multiplication
    size_reflected = DigitalSize(1024)
    reflected_result = 3 * size_reflected
    assert reflected_result.bytes_ == 3072


def test_divisions() -> None:
    # Division of DigitalSize by an int
    size = DigitalSize(2048)
    assert (size / 2) == 1024

    # Division of DigitalSize by another DigitalSize
    size1 = DigitalSize(2048)
    size2 = DigitalSize(1024)
    assert (size1 / size2) == 2

    # Division by zero with an int should raise ZeroDivisionError
    with pytest.raises(ZeroDivisionError):
        _ = size / 0

    # Division by zero when the divisor is a DigitalSize with zero bytes
    size_zero = DigitalSize(0)
    with pytest.raises(ZeroDivisionError):
        _ = size1 / size_zero
