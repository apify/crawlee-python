from __future__ import annotations

import json
import pathlib
from urllib.error import HTTPError
from urllib.request import urlopen

PACKAGE_NAME = 'crawlee'
REPO_ROOT = pathlib.Path(__file__).parent.resolve() / '..'
PYPROJECT_TOML_FILE_PATH = REPO_ROOT / 'pyproject.toml'


# Load the current version number from pyproject.toml
# It is on a line in the format `version = "1.2.3"`
def get_current_package_version() -> str:
    with open(PYPROJECT_TOML_FILE_PATH, encoding='utf-8') as pyproject_toml_file:
        for line in pyproject_toml_file:
            if line.startswith('version = '):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]
        else:  # noqa: PLW0120
            raise RuntimeError('Unable to find version string.')


# Write the given version number from pyproject.toml
# It replaces the version number on the line with the format `version = "1.2.3"`
def set_current_package_version(version: str) -> None:
    with open(PYPROJECT_TOML_FILE_PATH, 'r+', encoding='utf-8') as pyproject_toml_file:
        updated_pyproject_toml_file_lines = []
        version_string_found = False
        for line in pyproject_toml_file:
            if line.startswith('version = '):
                version_string_found = True
                line = f'version = "{version}"\n'  # noqa: PLW2901
            updated_pyproject_toml_file_lines.append(line)

        if not version_string_found:
            raise RuntimeError('Unable to find version string.')

        pyproject_toml_file.seek(0)
        pyproject_toml_file.write(''.join(updated_pyproject_toml_file_lines))
        pyproject_toml_file.truncate()


# Load the version numbers of the currently published versions from PyPI
def get_published_package_versions() -> list:
    package_info_url = f'https://pypi.org/pypi/{PACKAGE_NAME}/json'
    try:
        package_data = json.load(urlopen(package_info_url))  # noqa: S310
        published_versions = list(package_data['releases'].keys())
    # If the URL returns 404, it means the package has no releases yet (which is okay in our case)
    except HTTPError as exc:
        if exc.code != 404:
            raise
        published_versions = []
    return published_versions
