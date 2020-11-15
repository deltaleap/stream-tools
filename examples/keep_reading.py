import asyncio

import aioredis


async def main():
    r = await aioredis.create_redis('redis://localhost')
    while True:
        res1 = await r.xread(['test_stream_1'], count=1)
        res2 = await r.xread(['test_stream_2'], count=1)
        print(res1)
        print(res2)

    r.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
