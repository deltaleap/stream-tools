from __future__ import annotations
from typing import Dict
from typing import Tuple
from typing import AsyncGenerator

import aioredis

ReadMessageType = Tuple[bytes, bytes, Dict[bytes, bytes]]


class Stream:
    def __init__(self, stream_name: str) -> None:
        self.stream_name = str(stream_name)

    @property
    def name(self) -> str:
        return self.stream_name

    async def __aenter__(self):
        self.r = await aioredis.create_redis(
            'redis://localhost'
        )
        self.running = True
        return self

    async def __aexit__(
        self,
        exception_type: str,
        exception: str,
        traceback: str
    ) -> None:
        self.r.close()

    async def __aiter__(self) -> Stream:
        return self

    async def read(
        self,
        timeout: int = 1
    ) -> AsyncGenerator[ReadMessageType, None]:
        last_message_id = b'0'
        while self.running:
            res = await self.r.xread(
                [self.stream_name],
                latest_ids=[last_message_id],
                count=1
            )

            for row in res:
                last_message_id = row[1]
                yield row
