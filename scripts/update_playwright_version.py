#!/usr/bin/env python3
"""Bump the Playwright versions pinned by the project template's Dockerfile.

The template Dockerfile pins a shared Playwright version (a Jinja `# % set playwright_version =
'...'` line) that selects the standard Apify base image tags and their in-image
`playwright==<version>` pins. A version is only safe to pin once Apify has published the matching
base image for every standard variant, so those images' Docker Hub tags are the source of truth:
this picks the highest stable `<python>-<semver>` tag shared by those images, for the Python
version the template already uses, and rewrites the pinned version line if it is newer. The
Python version and Camoufox's separate compatibility pin are never changed.

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
TAGS_URL_TEMPLATE = 'https://hub.docker.com/v2/repositories/apify/{repository}/tags?page_size=100'
REQUEST_TIMEOUT_SECONDS = 30
CAMOUFOX_VERSION_VARIABLE = 'camoufox_playwright_version'
CAMOUFOX_REPOSITORY = 'actor-python-playwright-camoufox'
SHARED_VERSION_VARIABLE = 'playwright_version'

# The standard Apify image repositories selected by the automatically updated shared pin. Camoufox
# caps `playwright` below the latest release, so its explicit compatibility pin is excluded.
SHARED_VERSION_REPOSITORIES = (
    'actor-python-playwright',
    'actor-python-playwright-chrome',
    'actor-python-playwright-firefox',
    'actor-python-playwright-webkit',
)
# A pinned version line, e.g. `# % set playwright_version = '1.60.0'`.
VERSION_LINE_TEMPLATE = r"(# % set {variable} = ')([^']+)(')"
# The Python part of the base image tag, e.g. the `3.13` in `...:3.13-1.60.0`.
PYTHON_PREFIX = re.compile(r'python-playwright[a-z-]*:(\d+\.\d+)-')


def fetch_tags(repository: str) -> list[str]:
    """Return all tag names of an Apify base image repository, following pagination."""
    tags: list[str] = []
    url: str | None = TAGS_URL_TEMPLATE.format(repository=repository)
    while url:
        with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:  # noqa: S310
            payload = json.load(response)
        tags.extend(result['name'] for result in payload['results'])
        url = payload['next']
    return tags


def resolve_latest_version(repositories: tuple[str, ...], python_prefix: str) -> tuple[int, ...]:
    """Return the highest stable version that every one of the given repositories publishes."""
    # Keep only stable `MAJOR.MINOR.PATCH` versions built for the template's current Python line.
    tag_re = re.compile(rf'^{re.escape(python_prefix)}-(\d+\.\d+\.\d+)$')
    published = {
        repository: {
            tuple(int(part) for part in match.group(1).split('.'))
            for tag in fetch_tags(repository)
            if (match := tag_re.match(tag))
        }
        for repository in repositories
    }

    without_stable_tag = [repository for repository, versions in published.items() if not versions]
    if without_stable_tag:
        images = ', '.join(f'apify/{repository}' for repository in without_stable_tag)
        raise SystemExit(f'No stable {python_prefix}-<version> tag is published by: {images}.')

    shared = set.intersection(*published.values())
    if not shared:
        images = ', '.join(f'apify/{repository}' for repository in repositories)
        raise SystemExit(f'No {python_prefix}-<version> tag is shared by all of: {images}.')
    return max(shared)


def validate_pinned_version(repository: str, python_prefix: str, pinned_version: str) -> None:
    """Fail unless the repository publishes the exact pinned version for the Python line."""
    expected_tag = f'{python_prefix}-{pinned_version}'
    if expected_tag not in fetch_tags(repository):
        raise SystemExit(f'apify/{repository} does not publish the pinned tag {expected_tag}.')


def main() -> None:
    """Bump the pinned Playwright versions in the template Dockerfile if newer ones are available."""
    content = DOCKERFILE.read_text(encoding='utf-8')

    python_match = PYTHON_PREFIX.search(content)
    if not python_match:
        raise SystemExit(f'Python base image prefix not found in {DOCKERFILE}.')
    python_prefix = python_match.group(1)

    shared_version_line = re.compile(VERSION_LINE_TEMPLATE.format(variable=SHARED_VERSION_VARIABLE))
    shared_version_match = shared_version_line.search(content)
    if not shared_version_match:
        raise SystemExit(f'Pinned {SHARED_VERSION_VARIABLE} line not found in {DOCKERFILE}.')
    current = shared_version_match.group(2)
    pinned = tuple(int(part) for part in current.split('.'))

    latest = resolve_latest_version(SHARED_VERSION_REPOSITORIES, python_prefix)
    latest_str = '.'.join(str(part) for part in latest)
    updated = content
    if latest > pinned:
        updated = shared_version_line.sub(rf'\g<1>{latest_str}\g<3>', content)
        message = f'Bumped {SHARED_VERSION_VARIABLE}: {current} -> {latest_str}'
    elif latest == pinned:
        message = f'{SHARED_VERSION_VARIABLE} is already up to date ({current}).'
    else:
        # The pin is ahead of what the images publish, so the template would emit a `FROM`
        # line for a tag that does not exist. Fail here rather than hours later in an e2e job.
        images = ', '.join(f'apify/{repository}' for repository in SHARED_VERSION_REPOSITORIES)
        raise SystemExit(
            f'{SHARED_VERSION_VARIABLE} is pinned to {current}, but the newest tag published by all of '
            f'{images} is {latest_str}.'
        )

    camoufox_version_line = re.compile(VERSION_LINE_TEMPLATE.format(variable=CAMOUFOX_VERSION_VARIABLE))
    camoufox_version_match = camoufox_version_line.search(updated)
    if not camoufox_version_match:
        raise SystemExit(f'Pinned {CAMOUFOX_VERSION_VARIABLE} line not found in {DOCKERFILE}.')
    validate_pinned_version(CAMOUFOX_REPOSITORY, python_prefix, camoufox_version_match.group(2))

    # Only report the outcome once every validation has passed, so a failed run never prints a
    # bump or an up-to-date message it did not actually persist.
    if updated != content:
        DOCKERFILE.write_text(updated, encoding='utf-8')
    print(message)


if __name__ == '__main__':
    main()
