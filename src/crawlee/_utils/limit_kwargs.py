from collections import Counter
from typing import Any


def limit_kwarg_count(max_kwargs: int = 1, **kwargs: Any) -> None:
    """Limits the number of non None key word arguments to max_kwargs."""
    if len(kwargs) - Counter(kwargs.values())[None] > max_kwargs:
        raise ValueError(f'Only one of {", ".join(kwargs.keys())} can be specified, not multiple.')
