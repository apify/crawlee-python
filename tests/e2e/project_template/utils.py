import re
import shutil
import subprocess
from pathlib import Path
from typing import Literal


def patch_crawlee_version_in_project(
    project_path: Path, wheel_path: Path, package_manager: Literal['pip', 'uv', 'poetry']
) -> None:
    """Ensure that the test is using current version of the crawlee from the source and not from Pypi."""
    # Copy prepared .whl file
    shutil.copy(wheel_path, project_path)

    if package_manager in {'poetry', 'uv'}:
        _patch_crawlee_version_in_pyproject_toml_based_project(project_path, wheel_path)
    else:
        _patch_crawlee_version_in_requirements_txt_based_project(project_path, wheel_path)


def _patch_crawlee_version_in_requirements_txt_based_project(project_path: Path, wheel_path: Path) -> None:
    # Get any extras
    requirements_path = project_path / 'requirements.txt'
    with requirements_path.open() as f:
        requirements = f.read()
        crawlee_extras = re.findall(r'crawlee(\[.*\])', requirements)[0] or ''

    # Modify requirements.txt to use crawlee from wheel file instead of from Pypi
    with requirements_path.open() as f:
        modified_lines = []
        for line in f:
            if 'crawlee' in line:
                modified_lines.append(f'./{wheel_path.name}{crawlee_extras}\n')
            else:
                modified_lines.append(line)
    with requirements_path.open('w') as f:
        f.write(''.join(modified_lines))

    # Patch the dockerfile to have wheel file available
    dockerfile_path = project_path / 'Dockerfile'
    with dockerfile_path.open() as f:
        modified_lines = []
        for line in f:
            modified_lines.append(line)
            if line.startswith('COPY requirements.txt ./'):
                modified_lines.extend(
                    [
                        f'COPY {wheel_path.name} ./\n',
                        # If no crawlee version bump, pip might be lazy and take existing pre-installed crawlee version,
                        # make sure that one is patched as well.
                        f'RUN pip install ./{wheel_path.name}{crawlee_extras} --force-reinstall\n',
                    ]
                )
    with dockerfile_path.open('w') as f:
        f.write(''.join(modified_lines))


def _patch_crawlee_version_in_pyproject_toml_based_project(project_path: Path, wheel_path: Path) -> None:
    """Ensure that the test is using current version of the crawlee from the source and not from Pypi."""
    # Get any extras
    pyproject_path = project_path / 'pyproject.toml'
    with pyproject_path.open() as f:
        pyproject = f.read()
        crawlee_extras = re.findall(r'crawlee(\[.*\])', pyproject)[0] or ''

    # Inject crawlee wheel file to the docker image and update project to depend on it."""
    dockerfile_path = project_path / 'Dockerfile'
    with dockerfile_path.open() as f:
        modified_lines = []
        for line in f:
            modified_lines.append(line)
            if line.startswith('COPY pyproject.toml'):
                if 'uv.lock' in line:
                    package_manager = 'uv'
                elif 'poetry.lock' in line:
                    package_manager = 'poetry'
                else:
                    raise RuntimeError('This does not look like a uv or poetry based project.')

                # Create lock file that is expected by the docker to exist(Even though it wil be patched in the docker).
                subprocess.run(
                    args=[package_manager, 'lock'],
                    cwd=str(project_path),
                    check=True,
                    capture_output=True,
                )

                # Add command to copy .whl to the docker image and update project with it.
                # Patching in docker file due to the poetry not properly supporting relative paths for wheel packages
                # and so the absolute path(in the container) is generated when running `add` command in the container.
                modified_lines.extend(
                    [
                        f'COPY {wheel_path.name} ./\n',
                        # If no crawlee version bump, poetry might be lazy and take existing pre-installed crawlee
                        # version, make sure that one is patched as well.
                        f'RUN pip install ./{wheel_path.name}{crawlee_extras} --force-reinstall\n',
                        f'RUN {package_manager} add ./{wheel_path.name}{crawlee_extras}\n',
                        f'RUN {package_manager} lock\n',
                    ]
                )
    with dockerfile_path.open('w') as f:
        f.write(''.join(modified_lines))
