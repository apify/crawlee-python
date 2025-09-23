from collections import Counter
from typing import Any


def raise_if_too_many_kwargs(max_kwargs: int = 1, **kwargs: Any) -> None:
    """Raise ValueError if there are more kwargs then max_kwargs."""
    if len(kwargs) - Counter(kwargs.values())[None] > max_kwargs:
        kwargs_names = [f'"{kwarg_name}"' for kwarg_name in kwargs]
        raise ValueError(f'Only one of {", ".join(kwargs_names)} can be specified, not multiple.')
