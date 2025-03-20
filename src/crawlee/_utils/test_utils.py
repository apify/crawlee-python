import shutil
import subprocess
from pathlib import Path


def patch_crawlee_version_in_pyproject_toml_based_project(project_path: Path, wheel_path: Path) -> None:
    """Ensure that the integration test is using current version of the crawlee from the source and not from Pypi."""
    # Copy prepared .whl file
    shutil.copy(wheel_path, project_path)

    # Inject crawlee wheel file to the docker image un update project to depend on it."""
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
                modified_lines.extend(
                    [
                        'COPY {wheel_name} ./',
                        f'RUN {package_manager} add ./{wheel_path.name}',
                        f'RUN {package_manager} lock',
                    ]
                )
