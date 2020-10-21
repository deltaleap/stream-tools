import asyncio
import logging
import random

import creme
import uvloop

from stream_tools import Stream
from stream_tools import Streams


async def main():
    model = creme.linear_model.LogisticRegression()

    stream1 = Stream('test_stream_1')
    stream2 = Stream('test_stream_2')

    async with Streams([stream1, stream2]) as streams:
        async for value in streams.join('time_catch', 5):
            try:
                val_1 = float(value[b'test_stream_1'][1][b'val'])
                val_2 = float(value[b'test_stream_2'][1][b'val'])
            except KeyError as e:
                logging.warning(f'key error: {e}')
            else:
                model.fit_one(
                    {'f1': val_1, 'f2': val_2},
                    random.choice([True, False])
                )
                print("===")
                print(f'a: {model.intercept}')
                print(f'b1: {model.weights["f1"]}')
                print(f'b2: {model.weights["f2"]}')
                print("===")


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
