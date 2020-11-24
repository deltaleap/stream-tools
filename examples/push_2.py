import asyncio
import random

import aioredis


async def push(r: aioredis.Redis) -> None:
    while True:
        p = await r.xadd(
            'stream_2',
            {
                'val': random.random() + 1.5
            },
            max_len=5,
            exact_len=True
        )
        print(f'stram_2: {p.decode()}')
        await asyncio.sleep(2.1)


async def main() -> None:
    r = await aioredis.create_redis('redis://localhost')
    await push(r)
    r.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
