from __future__ import annotations
import asyncio
import time

from typing import Dict
from typing import Tuple
from typing import TYPE_CHECKING

import aioredis


JOIN = [
    'update_state',
    'time_catch',
    'timeframe'
]


State = Dict[bytes, Tuple[bytes, bytes]]
StateTime = Dict[bytes, int]

if TYPE_CHECKING:
    StreamQueue = asyncio.Queue[Tuple[bytes, bytes, bytes]]
else:
    StreamQueue = asyncio.Queue


class Join:
    def __init__(
        self,
        redis: aioredis.Redis,
        callback,
        join: str,
        *args
    ) -> None:
        self.redis = redis
        self.callback = callback

        if join in JOIN:
            self.join = str(join)
        else:
            raise ValueError('Wrong join type.')

        if join == 'time_catch':
            self.window = args[0] * 1000
            self.state: State = {}
            self.state_time: StateTime = {}

        if join == 'update_state':
            self.state = {}
            self.state_time = {}

        self.queue: StreamQueue = asyncio.Queue()
        asyncio.ensure_future(self.callback(self.queue))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.join == 'time_catch':
            return await self.time_catch()
        elif self.join == 'update_state':
            return await self.update_state()
        # elif self.join == 'timeframe':
        #     return await self.timeframe()  # TODO

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
        state_value: bytes
    ) -> None:
        self.state[state_key] = (state_id, state_value)

        new_state_time = int(state_id.decode().split('-')[0])
        self.state_time[state_key] = new_state_time

        # remove state props too old if compared with the given window
        for k in self.state_time.keys():
            if new_state_time - self.state_time[k] > self.window:
                del self.state[k]

    async def update_state(self) -> State:
        res = await self.queue.get()
        self._store_state(res[0], res[1], res[2])
        return self.state

    def _store_state(self, state_key, state_id, state_value) -> None:
        self.state[state_key] = (state_id, state_value)
        new_state_time = int(state_id.decode().split('-')[0])
        self.state_time[state_key] = new_state_time
