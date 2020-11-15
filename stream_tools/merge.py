import asyncio
from typing import Dict
from typing import Tuple
from typing import Union

import aioredis


StreamQueue = asyncio.Queue[Tuple[Union[bytes, Dict[bytes, bytes]]]]


class Merge:
    def __init__(self, redis: aioredis.Redis, callback):
        self.redis = redis
        self.callback = callback
        self.queue: StreamQueue = asyncio.Queue()
        asyncio.ensure_future(self.callback(self.queue))

    def __aiter__(self):
        return self

    async def __anext__(self):
        res = await self.queue.get()
        return res
