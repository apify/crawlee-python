from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import pytest
from jinja2 import Environment

from scripts import update_playwright_version

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

# Tags served by the fake Docker Hub, varying per repository so the tests can exercise which
# published version ends up shared across all four standard images.
TAGS_BY_REPOSITORY = {
    'actor-python-playwright': ['3.13-1.60.0', '3.13-1.61.0', '3.13-1.62.0'],
    'actor-python-playwright-chrome': ['3.13-1.60.0', '3.13-1.61.0'],
    'actor-python-playwright-firefox': ['3.13-1.60.0', '3.13-1.61.0', '3.13-1.62.0'],
    'actor-python-playwright-webkit': ['3.13-1.60.0', '3.13-1.61.0', '3.13-1.62.0'],
    'actor-python-playwright-camoufox': ['3.13-1.59.0', '3.13-1.60.0', '3.13-1.61.0'],
}
UNSTABLE_TAGS = ['3.13-beta', '3.13-latest']
EXPECTED_SHARED_VERSION = '1.61.0'
EXPECTED_CAMOUFOX_VERSION = '1.60.0'
SHARED_VERSION_VARIABLE = 'playwright_version'
CAMOUFOX_VERSION_VARIABLE = 'camoufox_playwright_version'
CHROME_REPOSITORY = 'actor-python-playwright-chrome'
CAMOUFOX_REPOSITORY = 'actor-python-playwright-camoufox'
UNPUBLISHED_VERSION = '1.62.0'
# Position of the repository name in a Docker Hub tags path, `/v2/repositories/apify/<name>/tags`.
REPOSITORY_PATH_INDEX = 4
# Cookiecutter renders the template with the Jinja settings declared by the template itself.
COOKIECUTTER_CONFIG = json.loads(
    (update_playwright_version.DOCKERFILE.parent.parent / 'cookiecutter.json').read_text(encoding='utf-8')
)


class FakeResponse:
    """Minimal stand-in for the context manager returned by `urllib.request.urlopen`."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        self._payload = payload

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode()


def make_urlopen(tags_by_repository: Mapping[str, list[str]]) -> Callable[..., FakeResponse]:
    """Build a `urlopen` replacement serving the given tags for each Apify image repository."""

    def urlopen(url: str, **_: object) -> FakeResponse:
        repository = urlparse(url).path.split('/')[REPOSITORY_PATH_INDEX]
        tags = tags_by_repository[repository]
        return FakeResponse({'results': [{'name': tag} for tag in tags], 'next': None})

    return urlopen


@pytest.fixture
def template_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Copy the template Dockerfile to a temporary path and point the script at the copy."""
    dockerfile = tmp_path / 'Dockerfile'
    dockerfile.write_text(update_playwright_version.DOCKERFILE.read_text(encoding='utf-8'), encoding='utf-8')
    monkeypatch.setattr(update_playwright_version, 'DOCKERFILE', dockerfile)
    return dockerfile


def render_dockerfile(content: str, crawler_type: str) -> str:
    """Render the template Dockerfile the way Cookiecutter does for the given crawler type."""
    template = Environment(**COOKIECUTTER_CONFIG['_jinja2_env_vars']).from_string(content)  # noqa: S701
    return template.render(
        cookiecutter={
            '__package_name': 'demo_project',
            'crawler_type': crawler_type,
            'package_manager': 'pip',
        }
    )


def test_pins_track_the_image_repositories_they_select(template_copy: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The shared pin advances to the newest version common to every image it selects, while Camoufox keeps its own,
    separately pinned version."""
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(TAGS_BY_REPOSITORY))

    update_playwright_version.main()

    content = template_copy.read_text(encoding='utf-8')
    assert f"# % set {SHARED_VERSION_VARIABLE} = '{EXPECTED_SHARED_VERSION}'" in content
    assert f"# % set {CAMOUFOX_VERSION_VARIABLE} = '{EXPECTED_CAMOUFOX_VERSION}'" in content


def test_reports_when_the_shared_pin_is_current(
    template_copy: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Prints an up-to-date message, instead of bumping, when the shared pin matches the latest shared version."""
    content = template_copy.read_text(encoding='utf-8')
    shared_line = re.compile(rf"(# % set {SHARED_VERSION_VARIABLE} = ')[^']+(')")
    template_copy.write_text(shared_line.sub(rf'\g<1>{EXPECTED_SHARED_VERSION}\g<2>', content), encoding='utf-8')
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(TAGS_BY_REPOSITORY))

    update_playwright_version.main()

    assert f'{SHARED_VERSION_VARIABLE} is already up to date ({EXPECTED_SHARED_VERSION}).' in capsys.readouterr().out


def test_generated_dockerfile_pins_camoufox_to_its_own_version(
    template_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rendered Dockerfile pins Camoufox to its own version and every other crawler type to the shared version."""
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(TAGS_BY_REPOSITORY))
    update_playwright_version.main()
    content = template_copy.read_text(encoding='utf-8')

    camoufox = render_dockerfile(content, 'playwright-camoufox')
    assert f'FROM apify/actor-python-playwright-camoufox:3.13-{EXPECTED_CAMOUFOX_VERSION}' in camoufox
    assert f'playwright=={EXPECTED_CAMOUFOX_VERSION}' in camoufox

    chrome = render_dockerfile(content, 'playwright-chrome')
    assert f'FROM apify/actor-python-playwright-chrome:3.13-{EXPECTED_SHARED_VERSION}' in chrome
    assert f'playwright=={EXPECTED_SHARED_VERSION}' in chrome


def test_fails_when_the_shared_pinned_version_line_is_missing(
    template_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exits when the Dockerfile has no pinned shared version line to read or update."""
    content = template_copy.read_text(encoding='utf-8')
    shared_line = re.compile(rf"# % set {SHARED_VERSION_VARIABLE} = '[^']+'\n")
    template_copy.write_text(shared_line.sub('', content), encoding='utf-8')
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(TAGS_BY_REPOSITORY))

    with pytest.raises(SystemExit, match=SHARED_VERSION_VARIABLE):
        update_playwright_version.main()


def test_fails_when_the_python_base_image_prefix_is_missing(
    template_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exits when the Dockerfile has no Python base image prefix to resolve tags for."""
    content = template_copy.read_text(encoding='utf-8')
    template_copy.write_text(content.replace('python-playwright', 'python-standard'), encoding='utf-8')
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(TAGS_BY_REPOSITORY))

    with pytest.raises(SystemExit, match='Python base image prefix'):
        update_playwright_version.main()


def test_fails_when_the_camoufox_pinned_version_line_is_missing(
    template_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exits when the Dockerfile has no pinned Camoufox version line to validate."""
    content = template_copy.read_text(encoding='utf-8')
    camoufox_line = re.compile(rf"# % set {CAMOUFOX_VERSION_VARIABLE} = '[^']+'\n")
    template_copy.write_text(camoufox_line.sub('', content), encoding='utf-8')
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(TAGS_BY_REPOSITORY))

    with pytest.raises(SystemExit, match=CAMOUFOX_VERSION_VARIABLE):
        update_playwright_version.main()


def test_fails_when_the_camoufox_pinned_tag_is_not_published(
    template_copy: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exits when Camoufox's pinned tag is not published, even if the shared pin would bump."""
    original = template_copy.read_text(encoding='utf-8')
    tags = {**TAGS_BY_REPOSITORY, CAMOUFOX_REPOSITORY: ['3.13-1.61.0']}
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(tags))

    with pytest.raises(SystemExit, match=f'{CAMOUFOX_REPOSITORY}.*3.13-{EXPECTED_CAMOUFOX_VERSION}'):
        update_playwright_version.main()

    # The shared pin's bump must not be persisted, nor reported, when the later Camoufox validation fails.
    assert template_copy.read_text(encoding='utf-8') == original
    assert capsys.readouterr().out == ''


@pytest.mark.usefixtures('template_copy')
def test_fails_when_an_image_has_no_stable_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exits identifying the image that publishes no stable tag for the Python line."""
    # Chrome publishes no stable tag at all, so the shared pin cannot be resolved.
    tags = {**TAGS_BY_REPOSITORY, CHROME_REPOSITORY: UNSTABLE_TAGS}
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(tags))

    with pytest.raises(SystemExit, match=CHROME_REPOSITORY):
        update_playwright_version.main()


@pytest.mark.usefixtures('template_copy')
def test_fails_when_no_version_is_shared_by_every_image(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exits when every image publishes a stable tag but no version is common to all of them."""
    # Chrome only publishes a version none of the other images have, so no version overlaps.
    tags = {**TAGS_BY_REPOSITORY, CHROME_REPOSITORY: ['3.13-1.63.0']}
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(tags))

    with pytest.raises(SystemExit, match=f'No 3.13-<version> tag is shared by all of.*{CHROME_REPOSITORY}'):
        update_playwright_version.main()


def test_fails_when_the_shared_pin_is_ahead_of_every_published_image(
    template_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exits, without persisting anything, when the shared pin is newer than any published tag."""
    content = template_copy.read_text(encoding='utf-8')
    shared_line = re.compile(rf"(# % set {SHARED_VERSION_VARIABLE} = ')[^']+(')")
    template_copy.write_text(shared_line.sub(rf'\g<1>{UNPUBLISHED_VERSION}\g<2>', content), encoding='utf-8')
    monkeypatch.setattr(update_playwright_version.urllib.request, 'urlopen', make_urlopen(TAGS_BY_REPOSITORY))

    with pytest.raises(SystemExit, match=f'{UNPUBLISHED_VERSION}.*{CHROME_REPOSITORY}'):
        update_playwright_version.main()

    assert f"# % set {SHARED_VERSION_VARIABLE} = '{UNPUBLISHED_VERSION}'" in template_copy.read_text(encoding='utf-8')
