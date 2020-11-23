import asyncio

from stream_tools import Stream


async def main() -> None:
    async with Stream("stream_1") as s:
        async for value in s.read():
            print(value)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
