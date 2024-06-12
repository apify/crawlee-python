from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_BYTES_PER_KB = 1024
_BYTES_PER_MB = _BYTES_PER_KB**2
_BYTES_PER_GB = _BYTES_PER_KB**3
_BYTES_PER_TB = _BYTES_PER_KB**4


@dataclass(frozen=True)
class ByteSize:
    """Represents a byte size."""

    bytes: int

    def __post_init__(self) -> None:
        if self.bytes < 0:
            raise ValueError('ByteSize cannot be negative')

    @classmethod
    def validate(cls, value: Any) -> ByteSize:
        if isinstance(value, ByteSize):
            return value

        if not isinstance(value, (float, int)):
            raise TypeError('Value must be numeric')

        return cls(int(value))

    @classmethod
    def from_kb(cls, kb: float) -> ByteSize:
        return cls(int(kb * _BYTES_PER_KB))

    @classmethod
    def from_mb(cls, mb: float) -> ByteSize:
        return cls(int(mb * _BYTES_PER_MB))

    @classmethod
    def from_gb(cls, gb: float) -> ByteSize:
        return cls(int(gb * _BYTES_PER_GB))

    @classmethod
    def from_tb(cls, tb: float) -> ByteSize:
        return cls(int(tb * _BYTES_PER_TB))

    def to_kb(self) -> float:
        return self.bytes / _BYTES_PER_KB

    def to_mb(self) -> float:
        return self.bytes / _BYTES_PER_MB

    def to_gb(self) -> float:
        return self.bytes / _BYTES_PER_GB

    def to_tb(self) -> float:
        return self.bytes / _BYTES_PER_TB

    def __str__(self) -> str:
        if self.bytes >= _BYTES_PER_TB:
            return f'{self.to_tb():.2f} TB'
        if self.bytes >= _BYTES_PER_GB:
            return f'{self.to_gb():.2f} GB'
        if self.bytes >= _BYTES_PER_MB:
            return f'{self.to_mb():.2f} MB'
        if self.bytes >= _BYTES_PER_KB:
            return f'{self.to_kb():.2f} KB'
        return f'{self.bytes} B'

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ByteSize):
            return self.bytes == other.bytes
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, ByteSize):
            return self.bytes < other.bytes
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, ByteSize):
            return self.bytes <= other.bytes
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, ByteSize):
            return self.bytes > other.bytes
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, ByteSize):
            return self.bytes >= other.bytes
        return NotImplemented

    def __add__(self, other: object) -> ByteSize:
        if isinstance(other, ByteSize):
            return ByteSize(self.bytes + other.bytes)
        return NotImplemented

    def __sub__(self, other: object) -> ByteSize:
        if isinstance(other, ByteSize):
            result = self.bytes - other.bytes
            if result < 0:
                raise ValueError('Resulting ByteSize cannot be negative')
            return ByteSize(result)
        return NotImplemented

    def __mul__(self, other: object) -> ByteSize:
        if isinstance(other, (int, float)):
            return ByteSize(int(self.bytes * other))

        return NotImplemented

    def __truediv__(self, other: object) -> float:
        if isinstance(other, ByteSize):
            if other.bytes == 0:
                raise ZeroDivisionError('Cannot divide by zero')
            return self.bytes / other.bytes

        return NotImplemented

    def __rmul__(self, other: object) -> ByteSize:
        return self.__mul__(other)
