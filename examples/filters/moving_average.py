import asyncio

import uvloop

from stream_tools import Stream
from stream_tools.filters import MovingAverage


async def main() -> None:
    stream = Stream('stream_1')
    async with stream:
        async for value in MovingAverage(stream, ('x', 3)):
            print(value)


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
