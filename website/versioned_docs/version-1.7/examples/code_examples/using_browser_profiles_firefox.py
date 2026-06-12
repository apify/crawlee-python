import asyncio
from pathlib import Path

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

# Replace this with your actual Firefox profile name
# Find it at about:profiles in Firefox
PROFILE_NAME = 'your-profile-name-here'

# Paths to Firefox profiles in your system (example for Windows)
# Use `about:profiles` to find your profile path
PROFILE_PATH = Path(
    Path.home(), 'AppData', 'Roaming', 'Mozilla', 'Firefox', 'Profiles', PROFILE_NAME
)


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Use Firefox browser type
        browser_type='firefox',
        # Disable fingerprints to use the profile as is
        fingerprint_generator=None,
        headless=False,
        # Path to your Firefox profile
        user_data_dir=PROFILE_PATH,
        browser_launch_options={
            'args': [
                # Required to avoid version conflicts
                '--allow-downgrade'
            ]
        },
    )

    @crawler.router.default_handler
    async def default_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Visiting {context.request.url}')

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
