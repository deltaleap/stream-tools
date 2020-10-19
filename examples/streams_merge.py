import asyncio

import uvloop

from stream_tools import Streams, Stream


async def main():
	stream1 = Stream('test_stream_1')
	stream2 = Stream('test_stream_2')
	async with Streams([stream1, stream2]) as streams:
		async for value in streams.merge():
		    # Return a tuple:
		    #  (stream name, record id, record content)
			print(f"{value[0].decode()}: {value[1].decode()}")


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
