# ruff: noqa: FA100 ASYNC100
import asyncio
from functools import wraps
from typing import Annotated, Any, Callable, Coroutine, List, Union

import httpx
import inquirer  # type: ignore
import typer
from cookiecutter.main import cookiecutter  # type: ignore

TEMPLATE_LIST_URL = 'https://api.github.com/repos/apify/crawlee-python/contents/templates'


def run_async(func: Callable[..., Coroutine]) -> Callable:
    """Decorates a coroutine function so that it is ran with `asyncio.run`."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        asyncio.run(func(*args, **kwargs))

    return wrapper


cli = typer.Typer()


@cli.callback()
def callback() -> None:
    """An empty callback to force typer into making a CLI with a single command."""


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
        templates_response = httpx.get(TEMPLATE_LIST_URL)
        template_choices: List[str] = [item['name'] for item in templates_response.json() if item['type'] == 'dir']
    else:
        template_choices = []

    answers = (
        inquirer.prompt(
            [
                inquirer.Text(
                    'project_name',
                    message='Name of the new project folder',
                    validate=lambda _, it: len(it) > 0,
                    ignore=project_name is not None,
                ),
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

    project_name = project_name or answers['project_name']
    template = template or answers['template']

    cookiecutter(
        'gh:apify/crawlee-python',
        directory=f'templates/{template}',
        no_input=True,
        extra_context={'project_name': project_name},
    )
