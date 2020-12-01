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
    """Merger class. Merge in a single row values received
    from two or more streams
    """
    def __init__(self, redis: aioredis.Redis, reader: Callable) -> None:
        """Initialize the merger and start running the reader function

        Args:
            redis (aioredis.Redis): the redis instance
            reader (Callable): reader function that will
                send merged the parameters to the internal queue
        """
        self.redis = redis
        self.reader = reader
        self.queue: StreamQueue = asyncio.Queue()
        asyncio.ensure_future(self.reader(self.queue))

    def __aiter__(self) -> Merge:
        """Get the merge iterator

        Returns:
            Merge: the merger instance
        """
        return self

    async def __anext__(self) -> StreamRecord:
        """ Get the next value from the streams

        Returns:
            StreamRecord: return the element from the streams
        """
        res = await self.queue.get()
        return res  # yield?
