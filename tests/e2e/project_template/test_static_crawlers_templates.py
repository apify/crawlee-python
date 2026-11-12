import json
import os
import re
import subprocess
from pathlib import Path
from typing import Literal

import pytest
from apify_client import ApifyClientAsync
from cookiecutter.main import cookiecutter

from crawlee._cli import default_start_url, template_directory
from crawlee._utils.crypto import crypto_random_object_id
from tests.e2e.project_template.utils import patch_crawlee_version_in_project

# To run these tests locally, make sure you have apify-cli installed and available in the path.
# https://docs.apify.com/cli/docs/installation


@pytest.mark.parametrize(
    'crawler_type',
    [
        pytest.param('playwright-camoufox', marks=pytest.mark.playwright_camoufox),
        pytest.param('playwright-chrome', marks=pytest.mark.playwright_chrome),
        pytest.param('playwright-firefox', marks=pytest.mark.playwright_firefox),
        pytest.param('playwright-webkit', marks=pytest.mark.playwright_webkit),
        pytest.param('playwright', marks=pytest.mark.playwright),
        pytest.param('parsel', marks=pytest.mark.parsel),
        pytest.param('beautifulsoup', marks=pytest.mark.beautifulsoup),
        pytest.param('adaptive-beautifulsoup', marks=pytest.mark.adaptive_beautifulsoup),
        pytest.param('adaptive-parsel', marks=pytest.mark.adaptive_parsel),
        pytest.param('stagehand', marks=pytest.mark.stagehand),
    ],
)
@pytest.mark.parametrize(
    'http_client',
    [
        pytest.param('httpx', marks=pytest.mark.httpx),
        pytest.param('curl-impersonate', marks=pytest.mark.curl_impersonate),
        pytest.param('impit', marks=pytest.mark.impit),
    ],
)
@pytest.mark.parametrize(
    'package_manager',
    [
        pytest.param('pip', marks=pytest.mark.pip),
        pytest.param('uv', marks=pytest.mark.uv),
        pytest.param('poetry', marks=pytest.mark.poetry),
    ],
)
@pytest.mark.flaky(
    reruns=3,
    reason='`apify push` occasionally hangs against the Apify platform; retries cover transient CLI/network flakes.',
)
async def test_static_crawler_actor_at_apify(
    tmp_path: Path,
    crawlee_wheel_path: Path,
    package_manager: Literal['pip', 'uv', 'poetry'],
    crawler_type: str,
    http_client: str,
) -> None:
    # Generate new actor name
    actor_name = f'crawlee-python-template-e2e-test-{crypto_random_object_id(8).lower()}'

    # Create project from template
    cookiecutter(
        template=str(template_directory),
        no_input=True,
        extra_context={
            'project_name': actor_name,
            'package_manager': package_manager,
            'crawler_type': crawler_type,
            'http_client': http_client,
            'enable_apify_integration': True,
            'start_url': default_start_url,
            'install_project': False,
        },
        output_dir=str(tmp_path),
    )

    project_path = tmp_path / actor_name
    patch_crawlee_version_in_project(
        project_path=project_path, wheel_path=crawlee_wheel_path, package_manager=package_manager
    )

    # Run the CLI with its own home directory, so the secrets file it reads lives in `tmp_path`. It authenticates
    # through `APIFY_TOKEN` and never touches a stored login. `GIT_CEILING_DIRECTORIES` keeps git from walking up
    # into the surrounding project's .git/ when running under a basetemp inside the repo (see --basetemp in the
    # e2e-templates-tests poe task).
    cli_home = tmp_path / 'apify_cli_home'
    cli_home.mkdir()
    cli_env = {
        **os.environ,
        'HOME': str(cli_home),
        'APIFY_TOKEN': os.environ['APIFY_TEST_USER_API_TOKEN'],
        'GIT_CEILING_DIRECTORIES': str(tmp_path),
    }

    def run_cli(*args: str) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(  # noqa: S603
            ['apify', *args],  # noqa: S607
            capture_output=True,
            check=False,
            cwd=project_path,
            env=cli_env,
            timeout=600,
        )

    # Print apify version for debugging purposes in rare cases of CLI failures
    subprocess.run(['apify', '--version'], check=True)  # noqa: ASYNC221, S607

    init_process = run_cli('init', '-y', actor_name)
    assert init_process.returncode == 0, f'`apify init` failed: {init_process.stderr.decode()}'

    # Stagehand reads the model API key from the environment at run time. `apify push` sets the env vars from
    # `actor.json` on the Actor version before it starts the build, and sends `@`-prefixed values as secrets.
    if crawler_type == 'stagehand':
        secrets_process = run_cli('secrets', 'add', 'openaiApiKey', os.environ['OPENAI_API_KEY'])
        assert secrets_process.returncode == 0, f'`apify secrets add` failed: {secrets_process.stderr.decode()}'

        actor_json_path = project_path / '.actor' / 'actor.json'
        actor_json = json.loads(actor_json_path.read_text(encoding='utf-8'))
        actor_json['environmentVariables'] = {'OPENAI_API_KEY': '@openaiApiKey'}
        actor_json_path.write_text(json.dumps(actor_json, indent=4), encoding='utf-8')

    # With `--json`, `apify push` prints the push result to stdout and the build log to stderr.
    push_process = run_cli('push', '--json')
    try:
        push_result = json.loads(push_process.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f'`apify push` did not print a JSON result.\n'
            f'stdout: {push_process.stdout.decode()}\n'
            f'stderr: {push_process.stderr.decode()}'
        ) from exc

    client = ApifyClientAsync(token=os.environ['APIFY_TEST_USER_API_TOKEN'])
    actor = client.actor(push_result['actor']['id'])

    # Run actor
    try:
        assert push_result['ok'], f'`apify push` failed: {push_result.get("error")}'

        started_run_data = await actor.start(memory_mbytes=8192, build=push_result['build']['number'])
        actor_run = client.run(started_run_data['id'])

        finished_run_data = await actor_run.wait_for_finish()
        actor_run_log = await actor_run.log().get()
    finally:
        # Delete the actor once it is no longer needed.
        await actor.delete()

    # Asserts
    additional_run_info = f'Full actor run log: {actor_run_log}'
    assert actor_run_log
    assert finished_run_data
    assert finished_run_data['status'] == 'SUCCEEDED', additional_run_info
    assert (
        'Crawler.stop() was called with following reason: The crawler has reached its limit of 10 requests per crawl.'
    ) in actor_run_log, additional_run_info
    assert int(re.findall(r'requests_finished\s*│\s*(\d*)', actor_run_log)[-1]) >= 10, additional_run_info
