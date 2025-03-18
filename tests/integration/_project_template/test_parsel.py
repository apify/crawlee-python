import asyncio
import contextlib
import os
import re
import subprocess
from pathlib import Path

import pytest
import readchar
from apify_client import ApifyClientAsync
from typer.testing import CliRunner

import crawlee._cli
from crawlee._utils.crypto import crypto_random_object_id

runner = CliRunner()
# temp for local tests
os.environ['PATH'] = '/home/pijukatel/.nvm/versions/node/v22.11.0/bin:' + os.getenv('PATH')


@contextlib.contextmanager
def _change_dir(new_dir: Path) -> None:
    """Temporary change working directory to new_dir. Not thread safe as working directory is process specific."""
    old_cwd = os.getcwd()
    os.chdir(new_dir)
    yield
    os.chdir(old_cwd)


def generate_unique_actor_name(label: str) -> str:
    """Generates a unique resource name, which will contain the given label."""
    label = label.replace('_', '-')
    return f'python-crawlee-integration-tests-{label}-generated-{crypto_random_object_id(8)}'


async def test_default_template_actor_at_apify(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with _change_dir(tmp_path):
        # Generate new actor name
        actor_name = generate_unique_actor_name('default')

        # Create project through cli
        mock_input = iter(
            [
                *actor_name,
                readchar.key.ENTER,
                readchar.key.ENTER,
                readchar.key.ENTER,
                readchar.key.ENTER,
                readchar.key.ENTER,
                readchar.key.ENTER,
            ]
        )
        monkeypatch.setattr(target=readchar, name='readkey', value=lambda: next(mock_input))
        runner.invoke(crawlee._cli.cli, ['create'])

        # Go to new actor directory created by cli
        with _change_dir(tmp_path / actor_name):
            # Actor init
            init_process = subprocess.Popen(['apify', 'init'], stdin=subprocess.PIPE)  # noqa: ASYNC220, S603, S607
            # Should be almost instant, but it is not. Keep it simple, no point in some polling solution.
            await asyncio.sleep(1)
            # Yes to unrecognized Python project. https://github.com/apify/apify-cli/issues/746
            init_process.stdin.write(b'\n')
            await asyncio.sleep(1)
            init_process.communicate(f'{actor_name}\n'.encode())

            # Actor push
            build_process = subprocess.run(['apify', 'push'], capture_output=True, check=False)  # noqa: ASYNC221, S603, S607

    # Get actor ID from build log
    actor_id_regexp = re.compile(r'https:\/\/console\.apify\.com\/actors\/(.*)#\/builds\/\d*\.\d*\.\d*')
    # Why is it in stderr and not in stdout???
    actor_id = re.findall(actor_id_regexp, build_process.stderr.decode())[0]

    # Run actor
    try:
        client = ApifyClientAsync(
            token=os.getenv('APIFY_TEST_USER_API_TOKEN'), api_url=os.getenv('APIFY_INTEGRATION_TESTS_API_URL')
        )
        actor = client.actor(actor_id)
        started_run_data = await actor.start()
        actor_run = client.run(started_run_data['id'])

        finished_run_data = await actor_run.wait_for_finish()
        actor_run_log = await actor_run.log().get()
    finally:
        # Delete the actor once it is no longer needed.
        await actor.delete()

    # Asserts
    assert finished_run_data['status'] == 'SUCCEEDED'
    assert (
        'Crawler.stop() was called with following reason: The crawler has reached its limit of 50 requests per crawl.'
    ) in actor_run_log
    assert int(re.findall(r'requests_finished\s*│\s*(\d*)', actor_run_log)[-1]) >= 50
