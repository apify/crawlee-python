import asyncio
# % if cookiecutter.http_client == 'curl-impersonate' and 'playwright' not in cookiecutter.crawler_type
import platform
# % endif
{{ '' }}
from .main import main

if __name__ == '__main__':
    # % if cookiecutter.http_client == 'curl-impersonate' and 'playwright' not in cookiecutter.crawler_type
    if platform.system() == 'Windows':
        # This mitigates a warning raised by curl-cffi.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # % endif
{{ '' }}
    asyncio.run(main())
