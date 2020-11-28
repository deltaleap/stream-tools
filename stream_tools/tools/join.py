from __future__ import annotations
import asyncio
import time

from collections import OrderedDict
from typing import Callable
from typing import Dict
from typing import Tuple
from typing import Union
from typing import TYPE_CHECKING

import aioredis


JOIN = ["update_state", "time_catch", "timeframe"]


StateTime = Dict[bytes, int]

if TYPE_CHECKING:
    StreamValue = OrderedDict[bytes, bytes]
    StreamRecord = Tuple[bytes, bytes, StreamValue]
    StreamQueue = asyncio.Queue[StreamRecord]
    State = Dict[bytes, Tuple[bytes, OrderedDict[bytes, bytes]]]
else:
    StreamValue = OrderedDict
    StreamRecord = Tuple[bytes, bytes, StreamValue]
    StreamQueue = asyncio.Queue
    State = Dict[bytes, Tuple[bytes, OrderedDict]]


class Join:
    """Joiner class. Join data from two or more streams
    """
    def __init__(
        self,
        redis: aioredis.Redis,
        reader: Callable,
        join: str,
        *args: Union[int, float]
    ) -> None:
        """Initialize the joiner and start running the reader function

        Args:
            redis (aioredis.Redis): the redis instance
            reader (Callable): reader function that will send values
                to the internal queue
            join (str): the join method

        Raises:
            ValueError: in case the join method passed is not a str type
            TypeError: in case no time window are provided for time-catch join
        """
        self.redis = redis
        self.reader = reader

        if join in JOIN:
            self.join = str(join)
        else:
            raise ValueError("Wrong join type.")

        if join == "time_catch":
            if len(args) == 0:
                raise TypeError("No time window provided.")

            self.window = float(args[0]) * 1000
            self.state: State = {}
            self.state_time: StateTime = {}

        if join == "update_state":
            self.state = {}
            self.state_time = {}

        self.queue: StreamQueue = asyncio.Queue()
        asyncio.ensure_future(self.reader(self.queue))

    def __aiter__(self) -> Join:
        """Get the joiner iterator

        Returns:
            Join: the joiner instance
        """
        return self

    async def __anext__(self) -> State:
        """Get the next joined value from the streams

        Returns:
            State: the updated state as a result of the join
        """
        if self.join == "time_catch":
            res = await self.time_catch()
        elif self.join == "update_state":
            res = await self.update_state()
        # elif self.join == 'timeframe':
        #     res = await self.timeframe()  # TODO
        return res

    async def time_catch(self) -> State:
        """Get the new value from the queue and push it to
            the (timed) state

        Returns:
            State: the updated state as a result of the timed join
        """
        res = await self.queue.get()
        join_time = int(time.time() * 1000)

        self._time_store_state(join_time, res[0], res[1], res[2])

        return self.state

    def _time_store_state(
        self,
        join_time: int,
        state_key: bytes,
        state_id: bytes,
        state_value: StreamValue,
    ) -> None:
        """Update the joiner state and the timestamp of the last state
            for new observation from the streams, and remove state too
            old if compared from the provided time window. 

        Args:
            join_time (int): the reference time in milliseconds
            state_key (bytes): the stream name of the new observation
            state_id (bytes): redis id (timestamp) for the new observation
            state_value (StreamValue): fields-values included in the
                new observation
        """
        self.state[state_key] = (state_id, state_value)

        new_state_time = int(state_id.decode().split("-")[0])
        self.state_time[state_key] = new_state_time

        # remove state props too old if compared with the given window
        for k in self.state_time.keys():
            if new_state_time - self.state_time[k] > self.window:
                del self.state[k]

    async def update_state(self) -> State:
        """Get the new value from the queue and push it to
            the (timed) state

        Returns:
            State: the updated state as a result of the plain join
        """
        res = await self.queue.get()
        self._store_state(res[0], res[1], res[2])
        return self.state

    def _store_state(
        self, state_key: bytes, state_id: bytes, state_value: StreamValue
    ) -> None:
        """Update the joiner state and the timestamp of the last state
            for new observation from the streams

        Args:
            state_key (bytes): the stream name of the new observation
            state_id (bytes): redis id (timestamp) of the new observation
            state_value (StreamValue): fields-values included in the new
                observation
        """
        self.state[state_key] = (state_id, state_value)
        new_state_time = int(state_id.decode().split("-")[0])
        self.state_time[state_key] = new_state_time
