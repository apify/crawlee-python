# ruff: noqa: FA100 ASYNC210 ASYNC100
import asyncio
from functools import wraps
from pathlib import Path
from typing import Annotated, Any, Callable, Coroutine, List, Union

import httpx
import inquirer  # type: ignore
import typer
from cookiecutter.main import cookiecutter  # type: ignore
from rich.progress import Progress, SpinnerColumn, TextColumn

TEMPLATE_LIST_URL = 'https://api.github.com/repos/apify/crawlee-python/contents/templates'


def run_async(func: Callable[..., Coroutine]) -> Callable:
    """Decorates a coroutine function so that it is ran with `asyncio.run`."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        asyncio.run(func(*args, **kwargs))

    return wrapper


cli = typer.Typer(no_args_is_help=True)


@cli.callback(invoke_without_command=True)
def callback(
    version: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            '-V',
            '--version',
            is_flag=True,
            help='Print Crawlee version',
        ),
    ] = False,
) -> None:
    """Implements the 'no command' behavior."""
    if version:
        from crawlee import __version__

        typer.echo(__version__)


@cli.command()
@run_async
async def create(
    project_name: Annotated[
        Union[str, None],
        typer.Argument(
            help='The name of the project and the directory that will be created to contain it. '
            'If none is given, you will be prompted.'
        ),
    ] = None,
    template: Annotated[
        Union[str, None],
        typer.Option(help='The template to be used to create the project. If none is given, you will be prompted.'),
    ] = None,
) -> None:
    """Bootstrap a new Crawlee project."""
    if template is None:
        templates_response = httpx.get(TEMPLATE_LIST_URL, timeout=httpx.Timeout(10))
        template_choices: List[str] = [item['name'] for item in templates_response.json() if item['type'] == 'dir']
    else:
        template_choices = []

    while project_name is None:
        answers = (
            inquirer.prompt(
                [
                    inquirer.Text(
                        'project_name',
                        message='Name of the new project folder',
                        validate=lambda _, it: len(it) > 0,
                        ignore=project_name is not None,
                    ),
                ]
            )
            or {}
        )

        project_path = Path.cwd() / answers['project_name']

        if project_path.exists():
            typer.echo(f'Folder {project_path} exists', err=True)
        else:
            project_name = answers['project_name']

    answers = (
        inquirer.prompt(
            [
                inquirer.List(
                    'template',
                    message='Please select the template for your new Crawlee project',
                    choices=[(choice[0].upper() + choice[1:], choice) for choice in template_choices],
                    ignore=template is not None,
                ),
            ]
        )
        or {}
    )

    template = template or answers['template']

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        transient=True,
    ) as progress:
        progress.add_task(description='Bootstrapping...', total=None)
        cookiecutter(
            'gh:apify/crawlee-python',
            directory=f'templates/{template}',
            no_input=True,
            extra_context={'project_name': project_name},
        )

    typer.echo(f'Your project was created in {Path.cwd() / project_name}')
    typer.echo(f'To run your project, run `cd {project_name}`, `poetry install` and `python -m {project_name}`')
