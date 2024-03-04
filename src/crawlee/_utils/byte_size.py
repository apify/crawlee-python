from __future__ import annotations

from dataclasses import dataclass

_BYTES_PER_KB = 1024
_BYTES_PER_MB = _BYTES_PER_KB**2
_BYTES_PER_GB = _BYTES_PER_KB**3
_BYTES_PER_TB = _BYTES_PER_KB**4


@dataclass
class ByteSize:
    """Represents a size in bytes."""

    bytes_: int

    def to_kb(self: ByteSize) -> float:
        return self.bytes_ / _BYTES_PER_KB

    def to_mb(self: ByteSize) -> float:
        return self.bytes_ / _BYTES_PER_MB

    def to_gb(self: ByteSize) -> float:
        return self.bytes_ / _BYTES_PER_GB

    def to_tb(self: ByteSize) -> float:
        return self.bytes_ / _BYTES_PER_TB

    def __str__(self: ByteSize) -> str:
        if self.bytes_ >= _BYTES_PER_TB:
            return f'{self.to_tb():.2f} TB'

        if self.bytes_ >= _BYTES_PER_GB:
            return f'{self.to_gb():.2f} GB'

        if self.bytes_ >= _BYTES_PER_MB:
            return f'{self.to_mb():.2f} MB'

        if self.bytes_ >= _BYTES_PER_KB:
            return f'{self.to_kb():.2f} KB'

        return f'{self.bytes_} Bytes'
