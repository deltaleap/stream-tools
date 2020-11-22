from __future__ import annotations
import asyncio

from typing import Tuple
from typing import Callable
from typing import TYPE_CHECKING

import aioredis


if TYPE_CHECKING:
    StreamQueue = asyncio.Queue[Tuple[bytes, bytes, bytes]]
else:
    StreamQueue = asyncio.Queue


class Merge:
    def __init__(self, redis: aioredis.Redis, callback: Callable) -> None:
        self.redis = redis
        self.callback = callback
        self.queue: StreamQueue = asyncio.Queue()
        asyncio.ensure_future(self.callback(self.queue))

    def __aiter__(self) -> Merge:
        return self

    async def __anext__(self):
        res = await self.queue.get()
        return res
