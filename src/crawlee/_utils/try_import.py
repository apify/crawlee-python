import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from types import ModuleType
from typing import Any


@contextmanager
def try_import(module_name: str, *symbol_names: str) -> Iterator[None]:
    """Context manager to attempt importing symbols into a module.

    If an `ImportError` is raised during the import, the symbol will be replaced with a `FailedImport` object.
    """
    try:
        yield
    except ImportError as e:
        for symbol_name in symbol_names:
            setattr(sys.modules[module_name], symbol_name, FailedImport(e.args[0]))


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
