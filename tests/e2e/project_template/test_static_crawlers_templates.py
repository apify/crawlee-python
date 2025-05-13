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
        pytest.param('playwright', marks=pytest.mark.playwright),
        pytest.param('parsel', marks=pytest.mark.parsel),
        pytest.param('beautifulsoup', marks=pytest.mark.beautifulsoup),
    ],
)
@pytest.mark.parametrize(
    'http_client',
    [
        pytest.param('httpx', marks=pytest.mark.httpx),
        pytest.param('curl-impersonate', marks=pytest.mark.curl_impersonate),
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
        },
        accept_hooks=False,  # Do not install the newly created environment.
        output_dir=tmp_path,
    )

    patch_crawlee_version_in_project(
        project_path=tmp_path / actor_name, wheel_path=crawlee_wheel_path, package_manager=package_manager
    )

    # Build actor using sequence of cli commands as the user would
    subprocess.run(  # noqa: ASYNC221, S603
        ['apify', 'login', '-t', os.environ['APIFY_TEST_USER_API_TOKEN']],  # noqa: S607
        capture_output=True,
        check=True,
        cwd=tmp_path / actor_name,
    )
    subprocess.run(['apify', 'init', '-y', actor_name], capture_output=True, check=True, cwd=tmp_path / actor_name)  # noqa: ASYNC221, S603, S607

    build_process = subprocess.run(['apify', 'push'], capture_output=True, check=False, cwd=tmp_path / actor_name)  # noqa: ASYNC221, S603, S607
    # Get actor ID from build log
    actor_id_regexp = re.compile(r'https:\/\/console\.apify\.com\/actors\/(.*)#\/builds\/\d*\.\d*\.\d*')

    if match := re.findall(actor_id_regexp, build_process.stderr.decode()):
        actor_id = match[0]
    else:
        raise AssertionError(f'Failed to find actor id in build log: {build_process.stderr.decode()}')

    client = ApifyClientAsync(token=os.getenv('APIFY_TEST_USER_API_TOKEN'))
    actor = client.actor(actor_id)

    # Run actor
    try:
        assert build_process.returncode == 0
        started_run_data = await actor.start(memory_mbytes=8192)
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
