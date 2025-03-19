import shutil
import subprocess
from pathlib import Path


def _inject_pip_install_crawlee_from_whl_to_docker_file(wheel_name: str, docker_file: Path, line_marker: str) -> None:
    """Modify docker file to have crawlee .whl file available before the marker text."""
    with open(docker_file) as f:
        modified_lines = []
        for line in f:
            if line.startswith(line_marker):
                # Add command to copy .whl to the docker image and pip install crawlee from it.
                modified_lines.append(f"""COPY {wheel_name} ./\nRUN pip install ./{wheel_name}\n""")
            modified_lines.append(line)

    with open(docker_file, 'w') as f:
        f.write(''.join(modified_lines))


def patch_crawlee_version_in_uv_project(project_path: Path, wheel_path: Path) -> None:
    """Ensure that the integration test is using current version of the crawlee from the source and not from Pypi."""
    # Copy prepared .whl file
    shutil.copy(wheel_path, project_path)

    # Update the docker file just before installing the project to have .whl file available.
    _inject_pip_install_crawlee_from_whl_to_docker_file(
        docker_file=project_path / 'Dockerfile', wheel_name=wheel_path.name, line_marker='COPY pyproject.toml uv.lock'
    )

    # Add crawlee .whl dependency to the project toml and regenerate the lock file.
    subprocess.run(
        args=['uv', 'add', wheel_path.name],
        cwd=str(project_path),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        args=['uv', 'lock'],
        cwd=str(project_path),
        check=True,
        capture_output=True,
    )
