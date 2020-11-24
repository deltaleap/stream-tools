from __future__ import annotations
import asyncio

from collections import OrderedDict
from typing import Tuple
from typing import Callable
from typing import TYPE_CHECKING

import aioredis


if TYPE_CHECKING:
    StreamRecord = Tuple[bytes, bytes, OrderedDict[bytes, bytes]]
    StreamQueue = asyncio.Queue[StreamRecord]
else:
    StreamRecord = Tuple[bytes, bytes, OrderedDict]
    StreamQueue = asyncio.Queue


class Merge:
    def __init__(self, redis: aioredis.Redis, callback: Callable) -> None:
        self.redis = redis
        self.callback = callback
        self.queue: StreamQueue = asyncio.Queue()
        asyncio.ensure_future(self.callback(self.queue))

    def __aiter__(self) -> Merge:
        return self

    async def __anext__(self) -> StreamRecord:
        res = await self.queue.get()
        return res  # yield?
