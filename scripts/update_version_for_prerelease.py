#!/usr/bin/env python3

from __future__ import annotations

import re
import sys

from utils import get_current_package_version, get_published_package_versions, set_current_package_version

# Checks whether the current package version number was not already used in a published release,
# and if not, modifies the package version number in pyproject.toml
# from a stable release version (X.Y.Z) to a prerelease version (X.Y.ZbN or X.Y.Z.aN or X.Y.Z.rcN)
if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError('You must pass the prerelease type as an argument to this script!')

    prerelease_type = sys.argv[1]
    if prerelease_type not in ['alpha', 'beta', 'rc']:
        raise RuntimeError(f'The prerelease type must be one of "alpha", "beta" or "rc", got "{prerelease_type}"!')

    if prerelease_type == 'alpha':
        prerelease_prefix = 'a'
    elif prerelease_type == 'beta':
        prerelease_prefix = 'b'
    elif prerelease_type == 'rc':
        prerelease_prefix = 'rc'

    current_version = get_current_package_version()

    # We can only transform a stable release version (X.Y.Z) to a prerelease version (X.Y.ZxxxN)
    if not re.match(r'^\d+\.\d+\.\d+$', current_version):
        raise RuntimeError(
            f'The current version {current_version} does not match the proper semver format for stable releases (X.Y.Z)'
        )

    # Load the version numbers of the currently published versions from PyPI
    published_versions = get_published_package_versions()

    # We don't want to publish a prerelease version with the same version number as an already released stable version
    if current_version in published_versions:
        raise RuntimeError(f'The current version {current_version} was already released!')

    # Find the highest prerelease version number that was already published
    latest_prerelease = 0
    for version in published_versions:
        if version.startswith(f'{current_version}{prerelease_prefix}'):
            prerelease_version = int(version.split(prerelease_prefix)[1])
            if prerelease_version > latest_prerelease:
                latest_prerelease = prerelease_version

    # Write the latest prerelease version number to pyproject.toml
    new_prerelease_version_number = f'{current_version}{prerelease_prefix}{latest_prerelease + 1}'
    set_current_package_version(new_prerelease_version_number)
