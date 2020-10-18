"""
This example work. Though, there is a problem.
'async for value in streams' is too generic and don't let
distinguish the api of this merge tool from the join tool.
To be more explicit and dev-friendly, it has to become as follows:
'async for value in streams.merge()'.
In this way it will possible to distinguish this merge tool api
from the join tool api ('async for value in streams.join()')
"""

import asyncio

import uvloop

from stream_tools import Streams, Stream


async def main():
	stream1 = Stream('test_stream_1')
	stream2 = Stream('test_stream_2')
	async with Streams([stream1, stream2]) as streams:
		async for value in streams.merge():
			print(f"{value[0].decode()}: {value[1].decode()}")


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
