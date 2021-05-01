import asyncio
import random

import aioredis


async def push(r: aioredis.Redis) -> None:
    while True:
        p = await r.xadd(
            'stream_1',
            {
                'val': random.random() + 0.5
            },
            maxlen=5,
            approximate=False
        )
        print(f'stream_1: {p.decode()}')
        await asyncio.sleep(.8)


async def main() -> None:
    r = aioredis.from_url('redis://localhost')
    await push(r)
    r.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
