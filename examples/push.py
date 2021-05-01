import asyncio
import random

import aioredis


async def push(r: aioredis.Redis, n: int) -> None:
    while True:
        p = await r.xadd(f"test_stream_{n}", {
                'val': random.random()
            },
            maxlen=2,
            approximate=False
        )
        print(f"{n}: {p.decode()}")
        await asyncio.sleep(.8 * n)


async def main() -> None:
    r = aioredis.from_url('redis://localhost')
    for i in range(1, 4):
        asyncio.create_task(push(r, i))

    while True:
        await asyncio.sleep(10)

    r.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
