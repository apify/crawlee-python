import asyncio

from crawlee.sessions import SessionPool


async def main() -> None:
    # Override the default Session pool configuration.
    async with SessionPool(
        max_pool_size=100,
        create_session_settings={'max_usage_count': 10, 'blocked_status_codes': [403]},
    ) as session_pool:
        session = await session_pool.get_session()

        # Increase the error_score.
        session.mark_bad()

        # Throw away the session.
        session.retire()

        # Lower the error_score and mark the session good.
        session.mark_good()


if __name__ == '__main__':
    asyncio.run(main())
