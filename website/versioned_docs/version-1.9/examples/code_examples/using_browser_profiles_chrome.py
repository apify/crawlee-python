import asyncio
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

# Profile name to use (usually 'Default' for single profile setups)
PROFILE_NAME = 'Default'

# Paths to Chrome profiles in your system (example for Windows)
# Use `chrome://version/` to find your profile path
PROFILE_PATH = Path(Path.home(), 'AppData', 'Local', 'Google', 'Chrome', 'User Data')


async def main() -> None:
    # Create a temporary folder to copy the profile to
    with TemporaryDirectory(prefix='crawlee-') as tmpdirname:
        tmp_profile_dir = Path(tmpdirname)

        # Copy the profile to a temporary folder
        shutil.copytree(
            PROFILE_PATH / PROFILE_NAME,
            tmp_profile_dir / PROFILE_NAME,
            dirs_exist_ok=True,
        )

        crawler = PlaywrightCrawler(
            headless=False,
            # Use the installed Chrome browser
            browser_type='chrome',
            # Disable fingerprints to preserve profile identity
            fingerprint_generator=None,
            # Set user data directory to temp folder
            user_data_dir=tmp_profile_dir,
            browser_launch_options={
                # Slow down actions to mimic human behavior
                'slow_mo': 200,
                'args': [
                    # Use the specified profile
                    f'--profile-directory={PROFILE_NAME}',
                ],
            },
        )

        @crawler.router.default_handler
        async def default_handler(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Visiting {context.request.url}')

        await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
