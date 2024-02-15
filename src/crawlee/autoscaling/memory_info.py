# Inspiration: https://github.com/apify/crawlee/blob/master/packages/utils/src/internals/memory-info.ts

from dataclasses import dataclass


@dataclass
class MemoryInfo:
    """Describes memory usage of the process."""

    total_bytes: int  # Total memory available in the system or container
    free_bytes: int  # Amount of free memory in the system or container
    used_bytes: int  # Amount of memory used (= totalBytes - freeBytes)
    main_process_bytes: int  # Amount of memory used by the current Python process
    child_processes_bytes: int  # Amount of memory used by child processes of the current Python process


async def get_memory_info() -> MemoryInfo:
    # TODO
    return MemoryInfo(1, 2, 3, 4, 5)
