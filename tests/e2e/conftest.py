import subprocess
from pathlib import Path

import pytest
from _pytest.config import Config
from filelock import FileLock

_CRAWLEE_ROOT_PATH = Path(__file__).parent.parent.parent.resolve()


def pytest_configure(config: Config) -> None:
    for marker in [
        'httpx',
        'curl_impersonate',
        'playwright',
        'playwright_camoufox',
        'parsel',
        'beautifulsoup',
        'uv',
        'poetry',
        'pip',
    ]:
        config.addinivalue_line('markers', f'{marker}: Integration test parameter marker.')


@pytest.fixture(scope='session')
def crawlee_wheel_path(tmp_path_factory: pytest.TempPathFactory, testrun_uid: str) -> Path:
    """Build the package wheel if it hasn't been built yet, and return the path to the wheel."""
    # Make sure the wheel is not being built concurrently across all the pytest-xdist runners,
    # through locking the building process with a temp file.
    with FileLock(tmp_path_factory.getbasetemp().parent / 'crawlee_wheel_build.lock'):
        # Make sure the wheel is built exactly once across all the pytest-xdist runners,
        # through an indicator file saying that the wheel was already built.
        was_wheel_built_this_test_run_file = tmp_path_factory.getbasetemp() / f'wheel_was_built_in_run_{testrun_uid}'
        if not was_wheel_built_this_test_run_file.exists():
            subprocess.run(
                args='python -m build',
                cwd=_CRAWLEE_ROOT_PATH,
                shell=True,
                check=True,
                capture_output=True,
            )
            was_wheel_built_this_test_run_file.touch()

        # Read the current package version, necessary for getting the right wheel filename.
        pyproject_toml_file = (_CRAWLEE_ROOT_PATH / 'pyproject.toml').read_text(encoding='utf-8')
        for line in pyproject_toml_file.splitlines():
            if line.startswith('version = '):
                delim = '"' if '"' in line else "'"
                crawlee_version = line.split(delim)[1]
                break
        else:
            raise RuntimeError('Unable to find version string.')

        wheel_path = _CRAWLEE_ROOT_PATH / 'dist' / f'crawlee-{crawlee_version}-py3-none-any.whl'

        # Just to be sure.
        assert wheel_path.exists()

        return wheel_path
