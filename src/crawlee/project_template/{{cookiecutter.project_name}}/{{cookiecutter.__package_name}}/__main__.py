import asyncio
# % if cookiecutter.http_client == 'curl-impersonate'
import platform
# % if 'playwright' in cookiecutter.crawler_type
import warnings
# % endif
# % endif
{{ '' }}
from .main import main

if __name__ == '__main__':
    # % if cookiecutter.http_client == 'curl-impersonate'
    if platform.system() == 'Windows':
        # This mitigates a warning raised by curl-cffi.
        # % if 'playwright' in cookiecutter.crawler_type
        warnings.warn(
            message=('curl-cffi suggests using WindowsSelectorEventLoopPolicy, but this conflicts with Playwright. '
                     'Ignore the curl-cffi warning.'),
            category=UserWarning,
            stacklevel=2,
        )
        # % else
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        # % endif
    # % endif
{{ '' }}
    asyncio.run(main())
