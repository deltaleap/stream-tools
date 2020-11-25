from __future__ import annotations

import asyncio
from collections import OrderedDict
from types import TracebackType
from typing import List
from typing import Type
from typing import Tuple
from typing import Optional
from typing import Union
from typing import TYPE_CHECKING

import aioredis

from .stream import Stream
from .tools.join import Join
from .tools.merge import Merge


if TYPE_CHECKING:
    StreamValue = OrderedDict[bytes, bytes]
    StreamRecord = Tuple[bytes, bytes, StreamValue]
    StreamQueue = asyncio.Queue[StreamRecord]
else:
    StreamQueue = asyncio.Queue


class Streams:
    def __init__(self, stream_list: List[Stream]) -> None:
        self.stream_list = list(stream_list)
        self.stream_names = [s.name for s in stream_list]

    async def __aenter__(self) -> Streams:
        self.redis = await aioredis.create_redis("redis://localhost")

        return self

    async def __aexit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self.redis.close()
        if isinstance(exception, RuntimeError):
            return True
        else:
            return False

    def merge(self) -> Merge:
        merger = Merge(self.redis, self._reads)
        return merger

    def join(self, join_method: str, *args: Union[int, float]) -> Join:
        joiner = Join(self.redis, self._reads, join_method, *args)
        return joiner

    async def _reads(self, queue: StreamQueue) -> None:
        while True:
            res = await self.redis.xread(self.stream_names, count=1)
            await queue.put(res[0])
