# ruff: noqa: TRY301, FBT002, UP007
from __future__ import annotations

import importlib.resources
import json
from pathlib import Path
from typing import Annotated, Optional, cast

import inquirer  # type: ignore[import-untyped]
import typer
from cookiecutter.main import cookiecutter  # type: ignore[import-untyped]
from inquirer.render.console import ConsoleRender  # type: ignore[import-untyped]
from rich.progress import Progress, SpinnerColumn, TextColumn

cli = typer.Typer(no_args_is_help=True)

template_directory = importlib.resources.files('crawlee') / 'project_template'
cookiecutter_json = json.load((template_directory / 'cookiecutter.json').open())

crawler_choices = cookiecutter_json['crawler_type']
http_client_choices = cookiecutter_json['http_client']
package_manager_choices = cookiecutter_json['package_manager']
default_start_url = cookiecutter_json['start_url']


@cli.callback(invoke_without_command=True)
def callback(
    version: Annotated[
        bool,
        typer.Option(
            '-V',
            '--version',
            is_flag=True,
            help='Print Crawlee version',
        ),
    ] = False,
) -> None:
    """Crawlee is a web scraping and browser automation library."""
    if version:
        from crawlee import __version__

        typer.echo(__version__)


def _prompt_for_project_name(initial_project_name: str | None) -> str:
    """Prompt the user for a non-empty project name that does not lead to an existing folder."""
    while True:
        if initial_project_name is not None:
            project_name = initial_project_name
            initial_project_name = None
        else:
            project_name = ConsoleRender().render(
                inquirer.Text(
                    name='project_name',
                    message='Name of the new project folder',
                    validate=lambda _, value: bool(value.strip()),
                ),
            )

        if not project_name:
            typer.echo('Project name is required.', err=True)
            continue

        project_path = Path.cwd() / project_name

        if project_path.exists():
            typer.echo(f'Folder {project_path} already exists. Please choose another name.', err=True)
            continue

        return project_name


def _prompt_text(message: str, default: str) -> str:
    return cast(
        str,
        ConsoleRender().render(
            inquirer.Text(
                name='text',
                message=message,
                default=default,
                validate=lambda _, value: bool(value.strip()),
            ),
        ),
    )


def _prompt_choice(message: str, choices: list[str]) -> str:
    """Prompt the user to pick one from a list of choices."""
    return cast(
        str,
        ConsoleRender().render(
            inquirer.List(
                name='choice',
                message=message,
                choices=[(choice[0].upper() + choice[1:], choice) for choice in choices],
            ),
        ),
    )


def _prompt_bool(message: str, *, default: bool) -> bool:
    return cast(
        bool,
        ConsoleRender().render(
            inquirer.Confirm(
                name='confirm',
                message=message,
                default=default,
            ),
        ),
    )


@cli.command()
def create(
    project_name: Optional[str] = typer.Argument(
        default=None,
        show_default=False,
        help='The name of the project and the directory that will be created to contain it. '
        'If none is given, you will be prompted.',
    ),
    crawler_type: Optional[str] = typer.Option(
        None,
        '--crawler-type',
        '--template',
        show_default=False,
        help='The library that will be used for crawling in your crawler. If none is given, you will be prompted.',
    ),
    http_client: Optional[str] = typer.Option(
        None,
        show_default=False,
        help='The library that will be used to make HTTP requests in your crawler. '
        'If none is given, you will be prompted.',
    ),
    package_manager: Optional[str] = typer.Option(
        default=None,
        show_default=False,
        help='Package manager to be used in the new project. If none is given, you will be prompted.',
    ),
    start_url: Optional[str] = typer.Option(
        default=None,
        show_default=False,
        help='The URL where crawling should start. If none is given, you will be prompted.',
    ),
    enable_apify_integration: Optional[bool] = typer.Option(
        None,
        '--apify/--no-apify',
        show_default=False,
        help='Should Apify integration be set up for you? If not given, you will be prompted.',
    ),
) -> None:
    """Bootstrap a new Crawlee project."""
    try:
        # Prompt for project name if not provided.
        project_name = _prompt_for_project_name(project_name)

        # Prompt for crawler_type if not provided.
        if crawler_type is None:
            crawler_type = _prompt_choice('Please select the Crawler type', crawler_choices)

        # Prompt for http_client if not provided.
        if http_client is None:
            http_client = _prompt_choice('Please select the HTTP client', http_client_choices)

        # Prompt for package manager if not provided.
        if package_manager is None:
            package_manager = _prompt_choice('Please select the package manager', package_manager_choices)

        # Prompt for start URL
        if start_url is None:
            start_url = _prompt_text('Please specify the start URL', default=default_start_url)

        # Ask about Apify integration if not explicitly configured
        if enable_apify_integration is None:
            enable_apify_integration = _prompt_bool('Should Apify integration be set up for you?', default=False)

        if all(
            [
                project_name,
                crawler_type,
                http_client,
                package_manager,
                start_url,
                enable_apify_integration is not None,
            ]
        ):
            package_name = project_name.replace('-', '_')

            # Start the bootstrap process.
            with Progress(
                SpinnerColumn(),
                TextColumn('[progress.description]{task.description}'),
                transient=True,
            ) as progress:
                progress.add_task(description='Bootstrapping...', total=None)
                cookiecutter(
                    template=str(template_directory),
                    no_input=True,
                    extra_context={
                        'project_name': project_name,
                        'package_manager': package_manager,
                        'crawler_type': crawler_type,
                        'http_client': http_client,
                        'enable_apify_integration': enable_apify_integration,
                        'start_url': start_url,
                    },
                )

            typer.echo(f'Your project "{project_name}" was created.')

            if package_manager == 'manual':
                typer.echo(
                    f'To run it, navigate to the directory: "cd {project_name}", '
                    f'install the dependencies listed in "requirements.txt" '
                    f'and run it using "python -m {package_name}".'
                )
            elif package_manager == 'pip':
                typer.echo(
                    f'To run it, navigate to the directory: "cd {project_name}", '
                    f'activate the virtual environment in ".venv" ("source .venv/bin/activate") '
                    f'and run your project using "python -m {package_name}".'
                )
            elif package_manager == 'poetry':
                typer.echo(
                    f'To run it, navigate to the directory: "cd {project_name}", '
                    f'and run it using "poetry run python -m {package_name}".'
                )

            typer.echo(f'See the "{project_name}/README.md" for more information.')

    except KeyboardInterrupt:
        typer.echo('Operation cancelled by user.')
