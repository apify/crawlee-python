# ruff: noqa: TRY301, FBT002, UP007
from __future__ import annotations

from pathlib import Path
from typing import Annotated, Union

import httpx
import inquirer  # type: ignore
import typer
from cookiecutter.main import cookiecutter  # type: ignore
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


@cli.command()
def create(
    project_name: Annotated[
        Union[str | None],
        typer.Argument(
            help='The name of the project and the directory that will be created to contain it. '
            'If none is given, you will be prompted.'
        ),
    ] = None,
    template: Annotated[
        Union[str | None],
        typer.Option(help='The template to be used to create the project. If none is given, you will be prompted.'),
    ] = None,
) -> None:
    """Bootstrap a new Crawlee project."""
    try:
        # Update template choices if template is not provided.
        if template is None:
            templates_response = httpx.get(TEMPLATE_LIST_URL, timeout=httpx.Timeout(10))
            template_choices: list[str] = [item['name'] for item in templates_response.json() if item['type'] == 'dir']
        else:
            template_choices = []

        # Get project name.
        if project_name is None:
            answers = (
                inquirer.prompt(
                    [
                        inquirer.Text(
                            name='project_name',
                            message='Name of the new project folder',
                            validate=lambda _, it: len(it) > 0,
                            ignore=project_name is not None,
                        ),
                    ]
                )
                or {}
            )

            project_name = answers.get('project_name')

            if project_name is None:
                typer.echo('Project name is required.', err=True)
                raise typer.Exit

        project_path = Path.cwd() / project_name

        if project_path.exists():
            typer.echo(f'Folder {project_path} exists, please choose another name.', err=True)
            raise typer.Exit

        # Get teamplate choice.

        if template is None:
            answers = (
                inquirer.prompt(
                    [
                        inquirer.List(
                            name='template',
                            message='Please select the template for your new Crawlee project',
                            choices=[(choice[0].upper() + choice[1:], choice) for choice in template_choices],
                            ignore=template is not None,
                        ),
                    ]
                )
                or {}
            )

            template = answers.get('template')

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

        typer.echo(f'Your project was created in {project_path}.')
        typer.echo(f'See the created `{project_name}/README.md` file for more information.')

    except KeyboardInterrupt:
        typer.echo('Operation cancelled by user.')


if __name__ == '__main__':
    cli()
