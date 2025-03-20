import os
import re
import subprocess
from pathlib import Path

import pytest
from apify_client import ApifyClientAsync
from cookiecutter.main import cookiecutter

from crawlee._cli import default_start_url,template_directory
from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.test_utils import patch_crawlee_version_in_pyproject_toml_based_project

@pytest.mark.parametrize("http_client", ["httpx", "curl-impersonate"])
@pytest.mark.parametrize("crawler_type", ["parsel", "beautifulsoup"])
@pytest.mark.parametrize("package_manager", ["uv", "poetry"])
async def test_static_crawler_actor_at_apify(tmp_path: Path,
                                                      crawlee_wheel_path: Path,
                                                      package_manager: str,
                                                      crawler_type: str,
                                                      http_client: str) -> None:
    # Generate new actor name
    actor_name = (f'crawlee-python-template-integration-test-'
                  f'{package_manager}-{crawler_type}-{http_client}-{crypto_random_object_id(8).lower()}')

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

    patch_crawlee_version_in_pyproject_toml_based_project(
        project_path=tmp_path / actor_name, wheel_path=crawlee_wheel_path)

    # Build actor using sequence of cli commands as the user would
    subprocess.run(  # noqa: ASYNC221, S603
        ['apify', 'login', '-t', os.environ['APIFY_TEST_USER_API_TOKEN']],  # noqa: S607
        capture_output=True,
        check=True,
        cwd=tmp_path / actor_name,
    )
    subprocess.run(['apify', 'init', '-y', actor_name], capture_output=True, check=True, cwd=tmp_path / actor_name)  # noqa: ASYNC221, S603, S607
    build_process = subprocess.run(['apify', 'push'], capture_output=True, check=True, cwd=tmp_path / actor_name)  # noqa: ASYNC221, S603, S607

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
    assert actor_run_log
    assert finished_run_data
    assert finished_run_data['status'] == 'SUCCEEDED'
    assert (
        'Crawler.stop() was called with following reason: The crawler has reached its limit of 50 requests per crawl.'
    ) in actor_run_log
    assert int(re.findall(r'requests_finished\s*│\s*(\d*)', actor_run_log)[-1]) >= 50
