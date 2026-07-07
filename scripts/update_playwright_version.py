#!/usr/bin/env python3
"""Bump the Playwright version pinned by the project template's Dockerfile.

The template Dockerfile pins a single Playwright version (a Jinja `# % set playwright_version
= '...'` line) that selects the Apify base image tag and the in-image `playwright==<version>`
pin. A version is only safe to pin once Apify has published the matching base image, so the
Apify Playwright base image's Docker Hub tags are the source of truth: this picks the highest
stable `<python>-<semver>` tag for the Python version the template already uses, and rewrites
the pinned version line if it is newer. The Python version itself is never changed.

Single-purpose: run with no arguments from anywhere in the repository.
"""

from __future__ import annotations

import json
import re
import urllib.request
from pathlib import Path

DOCKERFILE = (
    Path(__file__).resolve().parent.parent / 'src/crawlee/project_template/{{cookiecutter.project_name}}/Dockerfile'
)
TAGS_URL = 'https://hub.docker.com/v2/repositories/apify/actor-python-playwright/tags?page_size=100'

# The pinned version line, e.g. `# % set playwright_version = '1.60.0'`.
VERSION_LINE = re.compile(r"(# % set playwright_version = ')([^']+)(')")
# The Python part of the base image tag, e.g. the `3.13` in `...:3.13-1.60.0`.
PYTHON_PREFIX = re.compile(r'python-playwright[a-z-]*:(\d+\.\d+)-')


def fetch_tags() -> list[str]:
    """Return all tag names of the Apify Playwright base image, following pagination."""
    tags: list[str] = []
    url: str | None = TAGS_URL
    while url:
        with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
            payload = json.load(response)
        tags.extend(result['name'] for result in payload['results'])
        url = payload['next']
    return tags


def main() -> None:
    """Bump the pinned Playwright version in the template Dockerfile if a newer one is available."""
    content = DOCKERFILE.read_text(encoding='utf-8')
    version_match = VERSION_LINE.search(content)
    if not version_match:
        raise SystemExit(f'Pinned Playwright version line not found in {DOCKERFILE}.')
    current = version_match.group(2)

    python_match = PYTHON_PREFIX.search(content)
    if not python_match:
        raise SystemExit(f'Python base image prefix not found in {DOCKERFILE}.')
    python_prefix = python_match.group(1)
    # Keep only stable `MAJOR.MINOR.PATCH` versions built for the template's current Python line.
    tag_re = re.compile(rf'^{re.escape(python_prefix)}-(\d+\.\d+\.\d+)$')
    versions = [tuple(int(p) for p in m.group(1).split('.')) for tag in fetch_tags() if (m := tag_re.match(tag))]
    if not versions:
        raise SystemExit(f'No stable {python_prefix}-<version> base image tags found.')

    latest = max(versions)
    latest_str = '.'.join(str(part) for part in latest)
    if latest > tuple(int(part) for part in current.split('.')):
        DOCKERFILE.write_text(VERSION_LINE.sub(rf'\g<1>{latest_str}\g<3>', content), encoding='utf-8')
        print(f'Bumped Playwright version: {current} -> {latest_str}')
    else:
        print(f'Playwright version is already up to date ({current}).')


if __name__ == '__main__':
    main()
