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
    """Class implementing the basic redis stream
    """
    def __init__(self, stream_name: str) -> None:
        """Initialize the Stream

        Args:
            stream_name (str): the name of the redis stream
        """
        self.stream_name = str(stream_name)

    @property
    def name(self) -> str:
        """Get the name of the stream

        Returns:
            str: the stream name
        """
        return self.stream_name

    async def __aenter__(self) -> Stream:
        """Start the context of the stream.
        Entering in the context will create the connection with
        the redis server.

        Returns:
            Stream: the initialized stream
        """
        self.redis = await aioredis.create_redis("redis://localhost")
        self.running = True
        return self

    async def __aexit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        """Exiting the context of the stream.
        While exiting the connection with the redis server
        will be closed.

        Args:
            exception_type (Optional[Type[BaseException]]): the exception type
            exception (Optional[BaseException]): the exception
            traceback (Optional[TracebackType]): traceback message

        Returns:
            Optional[bool]: if the context is exited with a runtime error
        """
        self.redis.close()
        return bool(isinstance(exception, RuntimeError))

    async def __aiter__(self) -> Stream:
        """Return the Stream as an iterator.
        Call this with:
        > async for value in stream.read():
        >    print(value)

        Returns:
            Stream: the initialized stream
        """
        return self

    async def read(self, timeout: int = 1) -> AsyncGenerator[StreamRecord, None]:
        """Function to provide values from the stream in a async
        generator fashion.

        Args:
            timeout (int, optional): Timeout in seconds. Defaults to 1.

        Yields:
            Iterator[AsyncGenerator[StreamRecord, None]]: generator containing
                the read value
        """
        last_message_id = b"0"
        while self.running:
            res = await self.redis.xread(
                [self.stream_name], latest_ids=[last_message_id], count=1
            )

            for row in res:
                last_message_id = row[1]
                yield row

    async def _read(self, queue: StreamQueue) -> None:
        """Read the last value from the given stream
        and put it in the async queue.

        Args:
            queue (StreamQueue): the queue in which to put the read value
        """
        while True:
            res = await self.redis.xread([self.stream_name], count=1)
            await queue.put(res[0])
