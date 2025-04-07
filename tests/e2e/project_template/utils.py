import re
import shutil
import subprocess
from pathlib import Path


def patch_crawlee_version_in_pyproject_toml_based_project(project_path: Path, wheel_path: Path) -> None:
    """Ensure that the test is using current version of the crawlee from the source and not from Pypi."""
    # Copy prepared .whl file
    shutil.copy(wheel_path, project_path)

    # Get any extras
    with open(project_path / 'pyproject.toml') as f:
        pyproject = f.read()
        crawlee_extras = re.findall(r'crawlee(\[.*\])', pyproject)[0] or ''

    # Inject crawlee wheel file to the docker image and update project to depend on it."""
    with open(project_path / 'Dockerfile') as f:
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
                        f'COPY {wheel_path.name} ./',
                        # If no crawlee version bump, poetry might be lazy and take existing crawlee version,
                        # make sure that one is patched as well.
                        f'RUN pip install ./{wheel_path.name}{crawlee_extras} --force-reinstall',
                        f'RUN {package_manager} add ./{wheel_path.name}{crawlee_extras}',
                        f'RUN {package_manager} lock',
                    ]
                )
    with open(project_path / 'Dockerfile', 'w') as f:
        f.write('\n'.join(modified_lines))
