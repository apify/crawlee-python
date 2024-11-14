# ruff: noqa: TRY301, FBT002, UP007
from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Optional, cast

import httpx
import inquirer  # type: ignore
import typer
from cookiecutter.main import cookiecutter  # type: ignore
from inquirer.render.console import ConsoleRender  # type: ignore
from rich.progress import Progress, SpinnerColumn, TextColumn

TEMPLATE_LIST_URL = 'https://api.github.com/repos/apify/crawlee-python/contents/templates'

cli = typer.Typer(no_args_is_help=True)


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


def _prompt_for_template() -> str:
    """Prompt the user to select a template from a list."""
    # Fetch available templates
    response = httpx.get(
        TEMPLATE_LIST_URL,
        timeout=httpx.Timeout(10),
        headers=[('Authorization', f'Bearer {os.environ["GH_TOKEN"]}')] if 'GH_TOKEN' in os.environ else [],
    )
    response.raise_for_status()
    template_choices = [item['name'] for item in response.json() if item['type'] == 'dir']

    # Prompt for template choice
    return cast(
        str,
        ConsoleRender().render(
            inquirer.List(
                name='template',
                message='Please select the template for your new Crawlee project',
                choices=[(choice[0].upper() + choice[1:], choice) for choice in template_choices],
            ),
        ),
    )


@cli.command()
def create(
    project_name: Optional[str] = typer.Argument(
        default=None,
        help='The name of the project and the directory that will be created to contain it. '
        'If none is given, you will be prompted.',
        show_default=False,
    ),
    template: Optional[str] = typer.Option(
        default=None,
        help='The template to be used to create the project. If none is given, you will be prompted.',
        show_default=False,
    ),
) -> None:
    """Bootstrap a new Crawlee project."""
    try:
        # Prompt for project name if not provided.
        project_name = _prompt_for_project_name(project_name)

        # Prompt for template choice if not provided.
        if template is None:
            template = _prompt_for_template()

        if project_name and template:
            # Start the bootstrap process.
            with Progress(
                SpinnerColumn(),
                TextColumn('[progress.description]{task.description}'),
                transient=True,
            ) as progress:
                progress.add_task(description='Bootstrapping...', total=None)
                cookiecutter(
                    template='gh:apify/crawlee-python',
                    directory=f'templates/{template}',
                    no_input=True,
                    extra_context={'project_name': project_name},
                )

            typer.echo(f'Your project "{project_name}" was created.')
            typer.echo(
                f'To run it, navigate to the directory: "cd {project_name}", '
                'install dependencies with "poetry install", '
                f'and run it using "poetry run python -m {project_name}".'
            )
            typer.echo(f'See the "{project_name}/README.md" for more information.')

    except httpx.HTTPStatusError as exc:
        typer.echo(f'Failed to fetch templates: {exc}.', err=True)
    except KeyboardInterrupt:
        typer.echo('Operation cancelled by user.')
