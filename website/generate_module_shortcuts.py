#!/usr/bin/env python3

import importlib
import inspect
import json


def get_module_shortcuts(module, parent_classes=None):
    """Traverse a module and its submodules, and if some class is present in both a module and its submodule, register a shortcut."""
    shortcuts = {}

    if parent_classes is None:
        parent_classes = []
    parent_module_name = ".".join(module.__name__.split(".")[:-1])
    module_classes = []
    for classname, cls in inspect.getmembers(module, inspect.isclass):
        module_classes.append(cls)
        if cls in parent_classes:
            shortcuts[f"{module.__name__}.{classname}"] = (
                f"{parent_module_name}.{classname}"
            )

    for _, submodule in inspect.getmembers(module, inspect.ismodule):
        if submodule.__name__.startswith("apify"):
            shortcuts.update(get_module_shortcuts(submodule, module_classes))

    return shortcuts


def resolve_shortcuts(shortcuts):
    """Resolve linked shortcuts.

    For example, if there are shortcuts A -> B and B -> C,
    resolve them to A -> C.
    """
    for source, target in shortcuts.items():
        while target in shortcuts:
            shortcuts[source] = shortcuts[target]
            target = shortcuts[target]


shortcuts = {}
for module_name in ["crawlee"]:
    try:
        module = importlib.import_module(module_name)
        module_shortcuts = get_module_shortcuts(module)
        shortcuts.update(module_shortcuts)
    except ModuleNotFoundError:
        pass

resolve_shortcuts(shortcuts)

with open("module_shortcuts.json", "w", encoding="utf-8") as shortcuts_file:
    json.dump(shortcuts, shortcuts_file, indent=4, sort_keys=True)
