#!/usr/bin/env python3

"""
This script verifies the presence of a changelog entry in the `CHANGELOG.md` file for the current package version
in the `pyproject.toml`.
"""

from __future__ import annotations

import re

from utils import REPO_ROOT, get_current_package_version

CHANGELOG_PATH = REPO_ROOT / 'CHANGELOG.md'

if __name__ == '__main__':
    current_package_version = get_current_package_version()

    if not CHANGELOG_PATH.is_file():
        raise RuntimeError('Unable to find CHANGELOG.md file')

    with open(CHANGELOG_PATH, encoding='utf-8') as changelog_file:
        # Loop through the changelog lines to find a matching version heading
        for line in changelog_file:
            # Match version headings formatted as GitHub version tag links in square brackets
            if re.match(rf'## \[{current_package_version}\].*$', line):
                break
        else:
            raise RuntimeError(f'Changelog lacks entry for version {current_package_version}.')
