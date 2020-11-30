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
    """Implementing a class to manager two or more streams.
    """
    def __init__(self, stream_list: List[Stream]) -> None:
        """Initialize the set of streams.

        Args:
            stream_list (List[Stream]): the list of streams to be managed
        """
        self.stream_list = list(stream_list)
        self.stream_names = [s.name for s in stream_list]

    async def __aenter__(self) -> Streams:
        """Start the context of the set of streams.
        Entering the context will create the connection with
        the redis server

        Returns:
            Streams: the initialized set of streams
        """
        self.redis = await aioredis.create_redis("redis://localhost")

        return self

    async def __aexit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        """Exiting the context of the streams.
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

    def merge(self) -> Merge:
        """Return a merger as an iterator.
        > async value in streams.merge():
        >    print value

        Returns:
            Merge: the initialized merge class (an iterator)
        """
        merger = Merge(self.redis, self._reads)
        return merger

    def join(self, join_method: str, *args: Union[int, float]) -> Join:
        """Return a joiner as an iterator.
        > async value in streams.join('join_method', arg):
        >   print(value)

        Args:
            join_method (str): the join method.
                See ref to Join for more details

        Returns:
            Join: the initialized joiner class (an iterator)
        """
        joiner = Join(self.redis, self._reads, join_method, *args)
        return joiner

    async def _reads(self, queue: StreamQueue) -> None:
        """Read the last value out of all streams included in the set
        of streams and put it in the async queue

        Args:
            queue (StreamQueue): the queue in which to put the read value
        """
        while True:
            res = await self.redis.xread(self.stream_names, count=1)
            await queue.put(res[0])
