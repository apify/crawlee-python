import asyncio
import platform

from .main import main


if __name__ == '__main__':
    if platform.system() == 'Windows':
        # This mitigates a warning raised by curl-cffi. If you do not need to use curl-impersonate, you may remove this.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
