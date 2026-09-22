from __future__ import annotations

import sys
from types import ModuleType

import pytest

from crawlee._utils.try_import import FailedImport, install_import_hook, try_import


@pytest.fixture
def module_name(monkeypatch: pytest.MonkeyPatch) -> str:
    name = 'test_try_import_module'
    monkeypatch.setitem(sys.modules, name, ModuleType(name))
    return name


def test_missing_module_message_names_the_extra(module_name: str) -> None:
    """A missing optional dependency becomes a placeholder whose message tells the user which extra to install."""
    with try_import(module_name, 'OptionalSymbol', extra_name='parsel'):
        raise ModuleNotFoundError("No module named 'parsel'", name='parsel')

    placeholder = sys.modules[module_name].OptionalSymbol
    assert isinstance(placeholder, FailedImport)
    assert placeholder.message == (
        "No module named 'parsel'. Install the optional 'parsel' extra to use it: pip install 'crawlee[parsel]'"
    )


def test_missing_module_message_lists_alternative_extras(module_name: str) -> None:
    """When several extras provide the dependency, the message lists them and shows one install command."""
    with try_import(module_name, 'OptionalSymbol', extra_name=['sql_sqlite', 'sql_postgres', 'sql_mysql']):
        raise ModuleNotFoundError("No module named 'sqlalchemy'", name='sqlalchemy')

    assert sys.modules[module_name].OptionalSymbol.message == (
        "No module named 'sqlalchemy'. Install one of the optional extras 'sql_sqlite', 'sql_postgres', 'sql_mysql' "
        "to use it, e.g. pip install 'crawlee[sql_sqlite]'"
    )


def test_nested_guard_keeps_the_inner_message(module_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-exporting a guarded symbol through another guard does not append the install hint a second time."""
    inner_name = 'test_try_import_inner_module'
    monkeypatch.setitem(sys.modules, inner_name, ModuleType(inner_name))
    install_import_hook(inner_name)
    with try_import(inner_name, 'OptionalSymbol', extra_name='parsel'):
        raise ModuleNotFoundError("No module named 'parsel'", name='parsel')
    inner_message = vars(sys.modules[inner_name])['OptionalSymbol'].message

    with try_import(module_name, 'OptionalSymbol', extra_name='parsel'):
        getattr(sys.modules[inner_name], 'OptionalSymbol')  # noqa: B009

    assert sys.modules[module_name].OptionalSymbol.message == inner_message
    assert inner_message.count('pip install') == 1


def test_omitted_extra_keeps_the_original_message(module_name: str) -> None:
    """Without `extra_name` the message is left as it is, so external callers of the helper keep working."""
    with try_import(module_name, 'OptionalSymbol'):
        raise ModuleNotFoundError("No module named 'parsel'", name='parsel')

    assert sys.modules[module_name].OptionalSymbol.message == "No module named 'parsel'"


def test_install_hint_names_the_given_package(module_name: str) -> None:
    """Downstream packages reusing the helper can point users at their own extras."""
    with try_import(module_name, 'OptionalSymbol', extra_name='scrapy', package_name='apify'):
        raise ModuleNotFoundError("No module named 'scrapy'", name='scrapy')

    assert sys.modules[module_name].OptionalSymbol.message == (
        "No module named 'scrapy'. Install the optional 'scrapy' extra to use it: pip install 'apify[scrapy]'"
    )


def test_import_error_without_arguments_is_handled(module_name: str) -> None:
    """An `ImportError` carrying no message does not break the guard itself."""
    with try_import(module_name, 'OptionalSymbol', extra_name='parsel'):
        raise ImportError

    assert sys.modules[module_name].OptionalSymbol.message == ''
