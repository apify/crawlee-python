#!/usr/bin/env python3

"""
This script ensures that the package version specified in `pyproject.toml` is unique and not already published on PyPI.

It retrieves the current package version from `pyproject.toml` and compares it against the list of versions
already published on PyPI. If the current version already exists in the published versions, it raises a RuntimeError,
indicating that this version has already been released. This check helps avoid conflicts and errors during the
package publishing process.
"""

from __future__ import annotations

from utils import get_current_package_version, get_published_package_versions

if __name__ == '__main__':
    current_version = get_current_package_version()

    published_versions = get_published_package_versions()

    if current_version in published_versions:
        raise RuntimeError(f'The current version {current_version} was already released!')
