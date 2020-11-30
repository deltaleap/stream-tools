from __future__ import annotations

import asyncio

from collections import OrderedDict
from typing import Dict
from typing import Tuple
from typing import List
from typing import Union
from typing import Optional
from typing import TYPE_CHECKING

from ..stream import Stream

if TYPE_CHECKING:
    StreamValue = OrderedDict[bytes, bytes]
    StreamRecord = Tuple[bytes, bytes, StreamValue]
    StreamQueue = asyncio.Queue[StreamRecord]
else:
    StreamValue = OrderedDict
    StreamQueue = asyncio.Queue


class SumBarState:
    def __init__(self, thresholds: Dict[str, float]) -> None:
        self.thresholds = thresholds
        self.trigger = False
        self.output: Optional[Tuple[bytes, bytes, Dict[bytes, float]]] = None
        self.empty_state()

    def empty_state(self) -> None:
        self.state = {
            field.encode(): 0.0 for field in self.thresholds.keys()
        }

    def check_trigger(self) -> None:
        status = False
        for k, v in self.thresholds.items():
            try:
                if self.state[k.encode()] >= v:
                    status = True
            except KeyError:
                pass

        if status:
            self.trigger = True
            self.output = self.name, self.last_idx, self.state
        else:
            self.trigger = False
            self.output = None

    def update(self, new_record: Tuple[bytes, bytes, StreamValue]) -> None:
        name, idx, new_value = new_record

        self.name = f'sum({name.decode()})'.encode()
        self.last_idx = idx

        if self.trigger:
            self.empty_state()
            self.trigger = False

        for k in new_value.keys():
            try:
                self.state[k] = self.state[k] + float(new_value[k].decode())
            except KeyError:
                self.state[k] = float(new_value[k].decode())

        self.check_trigger()


class SumBar:
    def __init__(
        self,
        stream: Stream,
        threshold: Union[
            Tuple[str, Union[int, float]],
            List[Tuple[str, Union[int, float]]]
        ]
    ) -> None:
        self.stream = stream

        if isinstance(threshold, tuple):
            self.thresholds = {threshold[0]: threshold[1]}
        elif isinstance(threshold, list):
            self.thresholds = {t[0]: t[1] for t in threshold}
        else:  # TODO:  raise error if a list of other than tuples is provided
            raise TypeError('Sum threshold must be tuple or list of tuples')

        self.state = SumBarState(self.thresholds)

        self.queue: StreamQueue = asyncio.Queue()
        asyncio.ensure_future(self.stream._read(self.queue))

    @property
    def source_name(self) -> str:
        return self.stream.name

    @property
    def node_name(self) -> str:
        args = ", ".join([f"({k}, {v})" for k, v in self.thresholds.items()])
        node_name = f"sum_bar({self.source_name})[{args}]"
        return node_name

    def __aiter__(self) -> SumBar:
        return self

    async def __anext__(self) -> Optional[Tuple[bytes, bytes, Dict[bytes, float]]]:

        while True:
            res = await self.queue.get()

            self.state.update(res)

            if self.state.trigger:
                return self.state.output
