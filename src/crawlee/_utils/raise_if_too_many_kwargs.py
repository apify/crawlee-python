from typing import Any


def raise_if_too_many_kwargs(max_kwargs: int = 1, **kwargs: Any) -> None:
    """Raise ValueError if there are more non-None kwargs then max_kwargs."""
    none_kwargs_names = [f'"{kwarg_name}"' for kwarg_name, value in kwargs.items() if value is not None]
    if len(none_kwargs_names) > max_kwargs:
        all_kwargs_names = [f'"{kwarg_name}"' for kwarg_name in kwargs]
        raise ValueError(
            f'Only one of {", ".join(all_kwargs_names)} can be specified, but following arguments were '
            f'specified: {", ".join(none_kwargs_names)}.'
        )
