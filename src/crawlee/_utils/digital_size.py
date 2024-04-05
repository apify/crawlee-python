from __future__ import annotations

from dataclasses import dataclass

_BYTES_PER_KB = 1024
_BYTES_PER_MB = _BYTES_PER_KB**2
_BYTES_PER_GB = _BYTES_PER_KB**3
_BYTES_PER_TB = _BYTES_PER_KB**4


@dataclass(frozen=True)
class DigitalSize:
    """Represents a digital size."""

    bytes_: int

    def __post_init__(self) -> None:
        if self.bytes_ < 0:
            raise ValueError('DigitalSize cannot be negative')

    @classmethod
    def from_kb(cls, kb: float) -> DigitalSize:
        return cls(int(kb * _BYTES_PER_KB))

    @classmethod
    def from_mb(cls, mb: float) -> DigitalSize:
        return cls(int(mb * _BYTES_PER_MB))

    @classmethod
    def from_gb(cls, gb: float) -> DigitalSize:
        return cls(int(gb * _BYTES_PER_GB))

    @classmethod
    def from_tb(cls, tb: float) -> DigitalSize:
        return cls(int(tb * _BYTES_PER_TB))

    def to_kb(self) -> float:
        return self.bytes_ / _BYTES_PER_KB

    def to_mb(self) -> float:
        return self.bytes_ / _BYTES_PER_MB

    def to_gb(self) -> float:
        return self.bytes_ / _BYTES_PER_GB

    def to_tb(self) -> float:
        return self.bytes_ / _BYTES_PER_TB

    def __str__(self) -> str:
        if self.bytes_ >= _BYTES_PER_TB:
            return f'{self.to_tb():.2f} TB'
        if self.bytes_ >= _BYTES_PER_GB:
            return f'{self.to_gb():.2f} GB'
        if self.bytes_ >= _BYTES_PER_MB:
            return f'{self.to_mb():.2f} MB'
        if self.bytes_ >= _BYTES_PER_KB:
            return f'{self.to_kb():.2f} KB'
        return f'{self.bytes_} B'

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DigitalSize):
            return self.bytes_ == other.bytes_
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, DigitalSize):
            return self.bytes_ < other.bytes_
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, DigitalSize):
            return self.bytes_ <= other.bytes_
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, DigitalSize):
            return self.bytes_ > other.bytes_
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, DigitalSize):
            return self.bytes_ >= other.bytes_
        return NotImplemented

    def __add__(self, other: object) -> DigitalSize:
        if isinstance(other, DigitalSize):
            return DigitalSize(self.bytes_ + other.bytes_)
        return NotImplemented

    def __sub__(self, other: object) -> DigitalSize:
        if isinstance(other, DigitalSize):
            result = self.bytes_ - other.bytes_
            if result < 0:
                raise ValueError('Resulting DigitalSize cannot be negative')
            return DigitalSize(result)
        return NotImplemented

    def __mul__(self, other: object) -> DigitalSize:
        if isinstance(other, DigitalSize):
            return DigitalSize(self.bytes_ * other.bytes_)

        if isinstance(other, (int, float)):
            return DigitalSize(int(self.bytes_ * other))

        return NotImplemented

    def __truediv__(self, other: object) -> float:
        if isinstance(other, DigitalSize):
            if other.bytes_ == 0:
                raise ZeroDivisionError('Cannot divide by zero')
            return self.bytes_ / other.bytes_

        if isinstance(other, (int, float)):
            if other == 0:
                raise ZeroDivisionError('Cannot divide by zero')
            return self.bytes_ / other

        return NotImplemented

    def __rmul__(self, other: object) -> DigitalSize:
        return self.__mul__(other)
