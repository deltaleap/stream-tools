import asyncio
import random

import aioredis


async def push1(r):
    while True:
        p = await r.xadd(
            "test_stream_1", {"val": random.random()}, max_len=2, exact_len=True
        )
        print(f"test_stream_1: {p.decode()}")
        await asyncio.sleep(1)


async def push2(r):
    while True:
        p = await r.xadd(
            "test_stream_2", {"val": random.random()}, max_len=2, exact_len=True
        )
        print(f"test_stream_2: {p.decode()}")
        await asyncio.sleep(3)


async def main():
    r = await aioredis.create_redis("redis://localhost")
    await asyncio.gather(push1(r), push2(r))
    r.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
