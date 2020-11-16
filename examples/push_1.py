import asyncio
import random

import aioredis


async def push(r):
    while True:
        p = await r.xadd(
            "stream_1", {"val": random.random() + 0.5}, max_len=5, exact_len=True
        )
        print(f"stream_1: {p.decode()}")
        await asyncio.sleep(0.8)


async def main():
    r = await aioredis.create_redis("redis://localhost")
    await push(r)
    r.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
