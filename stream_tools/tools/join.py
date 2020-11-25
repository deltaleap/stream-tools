from __future__ import annotations
import asyncio
import time

from collections import OrderedDict
from typing import Callable
from typing import Dict
from typing import Tuple
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
    def __init__(
        self, redis: aioredis.Redis, callback: Callable, join: str, *args: Union[int, float]
    ) -> None:
        self.redis = redis
        self.callback = callback

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
        asyncio.ensure_future(self.callback(self.queue))

    def __aiter__(self) -> Join:
        return self

    async def __anext__(self) -> State:
        if self.join == "time_catch":
            res = await self.time_catch()
        elif self.join == "update_state":
            res = await self.update_state()
        # elif self.join == 'timeframe':
        #     res = await self.timeframe()  # TODO
        return res

    async def time_catch(self) -> State:
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
        self.state[state_key] = (state_id, state_value)

        new_state_time = int(state_id.decode().split("-")[0])
        self.state_time[state_key] = new_state_time

        # remove state props too old if compared with the given window
        for k in self.state_time.keys():
            if new_state_time - self.state_time[k] > self.window:
                del self.state[k]

    async def update_state(self) -> State:
        res = await self.queue.get()
        self._store_state(res[0], res[1], res[2])
        return self.state

    def _store_state(
        self, state_key: bytes, state_id: bytes, state_value: StreamValue
    ) -> None:
        self.state[state_key] = (state_id, state_value)
        new_state_time = int(state_id.decode().split("-")[0])
        self.state_time[state_key] = new_state_time
