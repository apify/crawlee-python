#!/usr/bin/env python3
"""Bump the Playwright version pinned by the cookiecutter project template's Dockerfile.

The template Dockerfile pins a single Playwright version (a Jinja ``# % set
playwright_version = '...'`` line). That version selects the Apify base image tag
(``apify/actor-python-playwright*:<python>-<playwright_version>``) and the in-image
``playwright==<playwright_version>`` pin, so the version is only safe to bump once Apify has
published the matching base image. This script therefore uses the Apify Playwright base image's
Docker Hub tags as the source of truth: it picks the highest stable ``<python>-<semver>`` tag,
compares it to the version currently in the Dockerfile, and rewrites that single line if a newer
one is available.

Intended to run from the repository root inside the daily ``update_playwright_version`` workflow,
but it is dependency-free (standard library only) and can be run locally.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path
from urllib.error import URLError

# Docker Hub repository whose tags gate which Playwright versions are safe to pin. The
# playwright/chrome/firefox/webkit/camoufox variants share the same ``<python>-<playwright>`` tag,
# so the primary repo is a sufficient reference for the shared version.
DOCKER_REPO = 'apify/actor-python-playwright'
DOCKER_HUB_TAGS_URL = f'https://hub.docker.com/v2/repositories/{DOCKER_REPO}/tags'

# Path (relative to the repository root) of the template Dockerfile that holds the pinned version.
DEFAULT_DOCKERFILE = Path('src/crawlee/project_template/{{cookiecutter.project_name}}/Dockerfile')

# The pinned version line, e.g. ``# % set playwright_version = '1.60.0'``.
VERSION_LINE_RE = re.compile(r"""(?P<prefix># % set playwright_version = ')(?P<version>[^']+)(?P<suffix>')""")

# The Python part of the base image tag, e.g. the ``3.13`` in ``...:3.13-1.60.0``.
PYTHON_PREFIX_RE = re.compile(r'python-playwright[a-z-]*:(?P<python>\d+\.\d+)-')

# A stable release version: exactly ``MAJOR.MINOR.PATCH`` with no pre-release/date suffix.
STABLE_VERSION_RE = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')

REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_version(version: str) -> tuple[int, int, int] | None:
    """Parse a stable ``MAJOR.MINOR.PATCH`` string into a comparable tuple, or None if not stable."""
    match = STABLE_VERSION_RE.match(version)
    if match is None:
        return None
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def read_current_version(dockerfile: Path) -> str:
    """Return the Playwright version currently pinned in the Dockerfile template."""
    content = dockerfile.read_text(encoding='utf-8')
    match = VERSION_LINE_RE.search(content)
    if match is None:
        raise ValueError(f'Could not find a `# % set playwright_version = ...` line in {dockerfile}')
    return match.group('version')


def read_python_prefix(dockerfile: Path, default: str = '3.13') -> str:
    """Return the Python version prefix used in the base image tag (e.g. ``3.13``)."""
    content = dockerfile.read_text(encoding='utf-8')
    match = PYTHON_PREFIX_RE.search(content)
    if match is None:
        return default
    return match.group('python')


def latest_stable_version(tags: list[str], python_prefix: str) -> str | None:
    """Pick the highest stable Playwright version among ``<python_prefix>-<semver>`` tags."""
    wanted = re.compile(rf'^{re.escape(python_prefix)}-(?P<version>\d+\.\d+\.\d+)$')
    best: tuple[int, int, int] | None = None
    best_str: str | None = None
    for tag in tags:
        match = wanted.match(tag)
        if match is None:
            continue
        version = match.group('version')
        parsed = parse_version(version)
        if parsed is None:
            continue
        if best is None or parsed > best:
            best, best_str = parsed, version
    return best_str


def fetch_tags(url: str = DOCKER_HUB_TAGS_URL, *, page_size: int = 100, max_pages: int = 50) -> list[str]:
    """Fetch all tag names for the Docker Hub repository, following pagination."""
    tags: list[str] = []
    next_url: str | None = f'{url}?page_size={page_size}'
    pages = 0
    while next_url and pages < max_pages:
        request = urllib.request.Request(next_url, headers={'User-Agent': 'crawlee-python-version-bumper'})  # noqa: S310
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            payload = json.loads(response.read().decode('utf-8'))
        tags.extend(result['name'] for result in payload.get('results', []))
        next_url = payload.get('next')
        pages += 1
    return tags


def bump_dockerfile(dockerfile: Path, new_version: str) -> bool:
    """Rewrite the pinned Playwright version line in place. Returns True if the file changed."""
    content = dockerfile.read_text(encoding='utf-8')
    new_content, count = VERSION_LINE_RE.subn(
        lambda match: f'{match.group("prefix")}{new_version}{match.group("suffix")}',
        content,
    )
    if count == 0:
        raise ValueError(f'Could not find a `# % set playwright_version = ...` line in {dockerfile}')
    if new_content == content:
        return False
    dockerfile.write_text(new_content, encoding='utf-8')
    return True


def write_github_output(path: Path, values: dict[str, str]) -> None:
    """Append ``key=value`` lines to the file referenced by ``$GITHUB_OUTPUT``."""
    with path.open('a', encoding='utf-8') as output:
        for key, value in values.items():
            output.write(f'{key}={value}\n')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--dockerfile',
        type=Path,
        default=REPO_ROOT / DEFAULT_DOCKERFILE,
        help='Path to the template Dockerfile that pins the Playwright version.',
    )
    parser.add_argument(
        '--github-output',
        type=Path,
        default=None,
        help='Path to the GitHub Actions $GITHUB_OUTPUT file; when set, changed/old/new are written to it.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Detect and report a newer version without editing the Dockerfile.',
    )
    args = parser.parse_args(argv)

    dockerfile: Path = args.dockerfile
    if not dockerfile.is_file():
        print(f'error: Dockerfile not found: {dockerfile}', file=sys.stderr)
        return 1

    current = read_current_version(dockerfile)
    python_prefix = read_python_prefix(dockerfile)
    print(f'Current pinned Playwright version: {current} (base image Python prefix: {python_prefix})')

    try:
        tags = fetch_tags()
    except (URLError, TimeoutError, OSError, ValueError) as exc:
        print(f'error: failed to fetch tags from Docker Hub: {exc}', file=sys.stderr)
        return 1

    latest = latest_stable_version(tags, python_prefix)
    if latest is None:
        print(f'error: no stable `{python_prefix}-<version>` tags found for {DOCKER_REPO}', file=sys.stderr)
        return 1
    print(f'Latest available Playwright base image version: {latest}')

    current_parsed = parse_version(current)
    latest_parsed = parse_version(latest)
    changed = False
    if current_parsed is None:
        print(f'warning: current version {current!r} is not a stable release; leaving it unchanged.')
    elif latest_parsed is not None and latest_parsed > current_parsed:
        if args.dry_run:
            print(f'Would bump Playwright version: {current} -> {latest} (dry run)')
            changed = True
        else:
            changed = bump_dockerfile(dockerfile, latest)
            print(f'Bumped Playwright version: {current} -> {latest}')
    else:
        print('Playwright version is already up to date.')

    if args.github_output is not None:
        write_github_output(
            args.github_output,
            {'changed': 'true' if changed else 'false', 'old': current, 'new': latest},
        )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
