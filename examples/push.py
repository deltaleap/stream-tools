import asyncio
import random

import aioredis


async def push(r: aioredis.Redis, n: int) -> None:
    while True:
        p = await r.xadd(f"test_stream_{n}", {
                'val': random.random()
            },
            max_len=2,
            exact_len=True
        )
        print(f"{n}: {p.decode()}")
        await asyncio.sleep(.8 * n)


async def main() -> None:
    r = await aioredis.create_redis('redis://localhost')
    await asyncio.gather(*[push(r, i) for i in range(1, 3)])
    r.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
