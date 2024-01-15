#!/usr/bin/env python3

from __future__ import annotations

from utils import get_current_package_version, get_published_package_versions

# Checks whether the current package version number was not already used in a published release.
if __name__ == '__main__':
    current_version = get_current_package_version()

    # Load the version numbers of the currently published versions from PyPI
    published_versions = get_published_package_versions()

    # We don't want to try to publish a version with the same version number as an already released stable version
    if current_version in published_versions:
        raise RuntimeError(f'The current version {current_version} was already released!')
