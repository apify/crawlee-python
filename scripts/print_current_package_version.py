#!/usr/bin/env python3

from __future__ import annotations

from utils import get_current_package_version

# Print the current package version from the pyproject.toml file to stdout
if __name__ == '__main__':
    print(get_current_package_version(), end='')
