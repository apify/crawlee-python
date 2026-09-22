from __future__ import annotations

import sys
from contextlib import contextmanager
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any


@contextmanager
def try_import(
    module_name: str,
    *symbol_names: str,
    extra_name: str | list[str] | None = None,
    package_name: str = 'crawlee',
) -> Iterator[None]:
    """Context manager to attempt importing symbols into a module.

    If an `ImportError` is raised during the import, the symbols are replaced with `FailedImport` objects. When the
    error is a `ModuleNotFoundError` and `extra_name` is given, the message also names the optional extra (or one of
    several) that installs the missing dependency. Other import errors, including those raised by a nested guard,
    keep their message as is.

    Args:
        module_name: The name of the module the symbols are imported into.
        symbol_names: The names of the symbols being imported.
        extra_name: The optional extra (or extras) providing the dependency. When omitted, the error message is
            left as it is.
        package_name: The distribution whose extras are named in the message. Defaults to this package, and exists
            so that downstream packages reusing this helper can point users at their own extras.
    """
    try:
        yield
    except ImportError as e:
        message = str(e)
        if isinstance(e, ModuleNotFoundError) and extra_name:
            message = f'{message}. {_get_install_hint(extra_name, package_name)}'
        for symbol_name in symbol_names:
            setattr(sys.modules[module_name], symbol_name, FailedImport(message))


def _get_install_hint(extra_name: str | list[str], package_name: str) -> str:
    """Build the sentence telling the user which extra installs the missing optional dependency."""
    extras = [extra_name] if isinstance(extra_name, str) else list(extra_name)
    if len(extras) == 1:
        return f"Install the optional '{extras[0]}' extra to use it: pip install '{package_name}[{extras[0]}]'"
    names = ', '.join(f"'{name}'" for name in extras)
    return f"Install one of the optional extras {names} to use it, e.g. pip install '{package_name}[{extras[0]}]'"


def install_import_hook(module_name: str) -> None:
    """Install an import hook for a specified module."""
    sys.modules[module_name].__class__ = ImportWrapper


@dataclass
class FailedImport:
    """Represent a placeholder for a failed import."""

    message: str
    """The error message associated with the failed import."""


class ImportWrapper(ModuleType):
    """A wrapper class for modules to handle attribute access for failed imports."""

    def __getattribute__(self, name: str) -> Any:
        result = super().__getattribute__(name)

        if isinstance(result, FailedImport):
            raise ImportError(result.message)  # noqa: TRY004

        return result
