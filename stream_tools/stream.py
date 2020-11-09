import aioredis  # type: ignore


class Stream:
    def __init__(self, stream_name):
        self.stream_name = str(stream_name)

    @property
    def name(self):
        return self.stream_name

    async def __aenter__(self):
        self.r = await aioredis.create_redis(
            'redis://localhost'
        )
        return self

    async def __aexit__(self, exception_type, exception, traceback):
        self.r.close()

    async def __aiter__(self):
        return self

    async def read(self):
        res = await self.r.xread(
            [self.stream_name],
            count=1
        )

        while res:
            for row in res:
                yield row

            res = await self.r.xread(
                [self.stream_name],
                count=1
            )
