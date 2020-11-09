import aioredis  # type: ignore

from .join import Join
from .merge import Merge


class Streams:
    def __init__(self, stream_list):
        self.stream_list = list(stream_list)
        self.stream_names = [s.name for s in stream_list]

    async def __aenter__(self):
        self.redis = await aioredis.create_redis(
            'redis://localhost'
        )

        return self

    async def __aexit__(self, exception_type, exception, traceback):
        self.redis.close()

    def merge(self):
        merger = Merge(self.redis, self._reads)
        return merger

    def join(self, join_method, *args):
        joiner = Join(self.redis, self._reads, join_method, *args)
        return joiner

    async def _reads(self, queue):
        while True:
            res = await self.redis.xread(
                self.stream_names,
                count=1
            )
            await queue.put(res[0])
