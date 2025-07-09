# ruff: noqa: FBT002
from __future__ import annotations

import importlib.resources
import json
import sys
from pathlib import Path
from typing import Annotated, cast

from click import Choice

try:
    import inquirer
    import typer
    from cookiecutter.main import cookiecutter
    from inquirer.render.console import ConsoleRender
    from rich.progress import Progress, SpinnerColumn, TextColumn
except ModuleNotFoundError as exc:
    raise ImportError(
        "Missing required dependencies for the Crawlee CLI. It looks like you're running 'crawlee' "
        "without the CLI extra. Try using 'crawlee[cli]' instead."
    ) from exc

cli = typer.Typer(no_args_is_help=True)

template_directory = importlib.resources.files('crawlee') / 'project_template'
with (template_directory / 'cookiecutter.json').open() as f:
    cookiecutter_json = json.load(f)

crawler_choices = cookiecutter_json['crawler_type']
http_client_choices = cookiecutter_json['http_client']
package_manager_choices = cookiecutter_json['package_manager']
default_start_url = cookiecutter_json['start_url']
default_enable_apify_integration = cookiecutter_json['enable_apify_integration']
default_install_project = cookiecutter_json['install_project']


@cli.callback(invoke_without_command=True)
def callback(
    version: Annotated[
        bool,
        typer.Option(
            '-V',
            '--version',
            help='Print Crawlee version',
        ),
    ] = False,
) -> None:
    """Crawlee is a web scraping and browser automation library."""
    if version:
        from crawlee import __version__  # noqa: PLC0415

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
        'str',
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
        'str',
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
        'bool',
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
    project_name: str | None = typer.Argument(
        default=None,
        show_default=False,
        help='The name of the project and the directory that will be created to contain it. '
        'If none is given, you will be prompted.',
    ),
    crawler_type: str | None = typer.Option(
        None,
        '--crawler-type',
        '--template',
        show_default=False,
        click_type=Choice(crawler_choices),
        help='The library that will be used for crawling in your crawler. If none is given, you will be prompted.',
    ),
    http_client: str | None = typer.Option(
        None,
        show_default=False,
        click_type=Choice(http_client_choices),
        help='The library that will be used to make HTTP requests in your crawler. '
        'If none is given, you will be prompted.',
    ),
    package_manager: str | None = typer.Option(
        default=None,
        show_default=False,
        click_type=Choice(package_manager_choices),
        help='Package manager to be used in the new project. If none is given, you will be prompted.',
    ),
    start_url: str | None = typer.Option(
        default=None,
        show_default=False,
        metavar='[START_URL]',
        help='The URL where crawling should start. If none is given, you will be prompted.',
    ),
    *,
    enable_apify_integration: bool | None = typer.Option(
        None,
        '--apify/--no-apify',
        show_default=False,
        help='Should Apify integration be set up for you? If not given, you will be prompted.',
    ),
    install_project: bool | None = typer.Option(
        None,
        '--install/--no-install',
        show_default=False,
        help='Should the project be installed now? If not given, you will be prompted.',
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
            enable_apify_integration = _prompt_bool(
                'Should Apify integration be set up for you?', default=default_enable_apify_integration
            )

        # Ask about installing the project
        if install_project is None:
            install_project = _prompt_bool('Should the project be installed now?', default=default_install_project)

        if all(
            [
                project_name,
                crawler_type,
                http_client,
                package_manager,
                start_url,
                enable_apify_integration is not None,
                install_project is not None,
            ]
        ):
            package_name = project_name.replace('-', '_')

            # Start the bootstrap process.
            with Progress(
                SpinnerColumn(),
                TextColumn('[progress.description]{task.description}'),
                transient=True,
            ) as progress:
                bootstrap_task = progress.add_task(description='Bootstrapping...', total=None)

                try:
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
                            'install_project': install_project,
                        },
                    )
                except Exception as exc:
                    progress.update(bootstrap_task, visible=False)
                    progress.refresh()

                    # Print just the last line of the error message (the actual error without traceback)
                    if 'Hook script failed' in str(exc):
                        typer.echo('Project creation failed. Check the error message above.', err=True)
                    else:
                        typer.echo(f'Project creation failed: {exc!s}', err=True)

                    sys.exit(1)

            typer.echo(f'Your project "{project_name}" was created.')

            if install_project:
                if package_manager == 'pip':
                    typer.echo(
                        f'To run it, navigate to the directory: "cd {project_name}", '
                        f'activate the virtual environment in ".venv" ("source .venv/bin/activate") '
                        f'and run your project using "python -m {package_name}".'
                    )
                else:
                    typer.echo(
                        f'To run it, navigate to the directory: "cd {project_name}", '
                        f'and run it using "{package_manager} run python -m {package_name}".'
                    )
            elif package_manager == 'pip':
                typer.echo(
                    f'To run it, navigate to the directory: "cd {project_name}", '
                    f'install the dependencies listed in "requirements.txt" '
                    f'and run it using "python -m {package_name}".'
                )
            else:
                install_command = 'sync' if package_manager == 'uv' else 'install'
                typer.echo(
                    f'To run it, navigate to the directory: "cd {project_name}", '
                    f'install the project using "{package_manager} {install_command}", '
                    f'and run it using "{package_manager} run python -m {package_name}".'
                )

            typer.echo(f'See the "{project_name}/README.md" for more information.')

    except KeyboardInterrupt:
        typer.echo('Operation cancelled by user.')
