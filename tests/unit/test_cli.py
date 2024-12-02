from __future__ import annotations

import os
from unittest.mock import ANY, Mock

import pytest
import readchar
from typer.testing import CliRunner

import crawlee._cli

runner = CliRunner()


@pytest.fixture
def mock_cookiecutter(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mock_cookiecutter = Mock()
    monkeypatch.setattr(target=crawlee._cli, name='cookiecutter', value=mock_cookiecutter)

    return mock_cookiecutter


def test_create_interactive(mock_cookiecutter: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
    mock_input = iter(
        [
            *'my_project',
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
        ]
    )
    monkeypatch.setattr(target=readchar, name='readkey', value=lambda: next(mock_input))

    result = runner.invoke(crawlee._cli.cli, ['create'])
    assert 'Your project "my_project" was created.' in result.output

    mock_cookiecutter.assert_called_with(
        template=ANY,
        no_input=True,
        extra_context={
            'project_name': 'my_project',
            'package_manager': 'poetry',
            'crawler_type': 'beautifulsoup',
            'http_client': 'httpx',
            'enable_apify_integration': False,
            'start_url': 'https://crawlee.dev',
        },
    )


def test_create_interactive_non_default_template(mock_cookiecutter: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
    mock_input = iter(
        [
            *'my_project',
            readchar.key.ENTER,
            readchar.key.DOWN,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
        ]
    )
    monkeypatch.setattr(target=readchar, name='readkey', value=lambda: next(mock_input))

    result = runner.invoke(crawlee._cli.cli, ['create'])
    assert 'Your project "my_project" was created.' in result.output

    mock_cookiecutter.assert_called_with(
        template=ANY,
        no_input=True,
        extra_context={
            'project_name': 'my_project',
            'package_manager': 'poetry',
            'crawler_type': 'parsel',
            'http_client': 'httpx',
            'enable_apify_integration': False,
            'start_url': 'https://crawlee.dev',
        },
    )


def test_create_non_interactive(mock_cookiecutter: Mock) -> None:
    runner.invoke(
        crawlee._cli.cli,
        [
            'create',
            'my_project',
            '--crawler-type',
            'playwright',
            '--http-client',
            'curl-impersonate',
            '--package-manager',
            'pip',
            '--start-url',
            'https://yr.no',
            '--no-apify',
        ],
    )

    mock_cookiecutter.assert_called_with(
        template=ANY,
        no_input=True,
        extra_context={
            'project_name': 'my_project',
            'package_manager': 'pip',
            'crawler_type': 'playwright',
            'http_client': 'curl-impersonate',
            'start_url': 'https://yr.no',
            'enable_apify_integration': False,
        },
    )


def test_create_existing_folder(
    mock_cookiecutter: Mock, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> None:
    mock_input = iter(
        [
            *'my_project',
            readchar.key.ENTER,
        ]
    )
    monkeypatch.setattr(target=readchar, name='readkey', value=lambda: next(mock_input))

    tmp = tmp_path_factory.mktemp('workdir')
    os.chdir(tmp)
    (tmp / 'existing_project').mkdir()

    result = runner.invoke(
        crawlee._cli.cli,
        [
            'create',
            'existing_project',
            '--crawler-type',
            'playwright',
            '--http-client',
            'curl-impersonate',
            '--package-manager',
            'pip',
            '--start-url',
            'https://yr.no',
            '--no-apify',
        ],
    )
    assert 'existing_project already exists' in result.output

    mock_cookiecutter.assert_called_with(
        template=ANY,
        no_input=True,
        extra_context={
            'project_name': 'my_project',
            'package_manager': 'pip',
            'crawler_type': 'playwright',
            'http_client': 'curl-impersonate',
            'start_url': 'https://yr.no',
            'enable_apify_integration': False,
        },
    )


def test_create_existing_folder_interactive(
    mock_cookiecutter: Mock, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> None:
    mock_input = iter(
        [
            *'existing_project',
            readchar.key.ENTER,
            *'my_project',
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
        ]
    )
    monkeypatch.setattr(target=readchar, name='readkey', value=lambda: next(mock_input))

    tmp = tmp_path_factory.mktemp('workdir')
    os.chdir(tmp)
    (tmp / 'existing_project').mkdir()

    result = runner.invoke(crawlee._cli.cli, ['create', '--template', 'playwright'])
    assert 'existing_project already exists' in result.output

    mock_cookiecutter.assert_called_with(
        template=ANY,
        no_input=True,
        extra_context={
            'project_name': 'my_project',
            'package_manager': 'poetry',
            'crawler_type': 'playwright',
            'http_client': 'httpx',
            'start_url': 'https://crawlee.dev',
            'enable_apify_integration': False,
        },
    )


def test_create_existing_folder_interactive_multiple_attempts(
    mock_cookiecutter: Mock, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> None:
    mock_input = iter(
        [
            *'existing_project',
            readchar.key.ENTER,
            *'existing_project_2',
            readchar.key.ENTER,
            *'my_project',
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
            readchar.key.ENTER,
        ]
    )
    monkeypatch.setattr(target=readchar, name='readkey', value=lambda: next(mock_input))

    tmp = tmp_path_factory.mktemp('workdir')
    os.chdir(tmp)
    (tmp / 'existing_project').mkdir()
    (tmp / 'existing_project_2').mkdir()

    result = runner.invoke(crawlee._cli.cli, ['create', '--crawler-type', 'playwright'])
    assert 'existing_project already exists' in result.output

    mock_cookiecutter.assert_called_with(
        template=ANY,
        no_input=True,
        extra_context={
            'project_name': 'my_project',
            'package_manager': 'poetry',
            'crawler_type': 'playwright',
            'http_client': 'httpx',
            'start_url': 'https://crawlee.dev',
            'enable_apify_integration': False,
        },
    )
