from __future__ import annotations

import asyncio

from collections import OrderedDict
from types import TracebackType
from typing import Type
from typing import Tuple
from typing import AsyncGenerator
from typing import Optional
from typing import TYPE_CHECKING

import aioredis

if TYPE_CHECKING:
    StreamValue = OrderedDict[bytes, bytes]
    StreamRecord = Tuple[bytes, bytes, StreamValue]
    StreamQueue = asyncio.Queue[StreamRecord]
else:
    StreamValue = OrderedDict
    StreamRecord = Tuple[bytes, bytes, StreamValue]
    StreamQueue = asyncio.Queue


class Stream:
    def __init__(self, stream_name: str) -> None:
        self.stream_name = str(stream_name)

    @property
    def name(self) -> str:
        return self.stream_name

    async def __aenter__(self) -> Stream:
        self.redis = await aioredis.create_redis("redis://localhost")
        self.running = True
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

    async def __aiter__(self) -> Stream:
        return self

    async def read(self, timeout: int = 1) -> AsyncGenerator[StreamRecord, None]:
        last_message_id = b"0"
        while self.running:
            res = await self.redis.xread(
                [self.stream_name], latest_ids=[last_message_id], count=1
            )

            for row in res:
                last_message_id = row[1]
                yield row

    async def _read(self, queue: StreamQueue) -> None:
        while True:
            res = await self.redis.xread([self.stream_name], count=1)
            await queue.put(res[0])
