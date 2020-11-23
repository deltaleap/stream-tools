from __future__ import annotations
import asyncio
from collections import OrderedDict
from typing import Union
from typing import Dict
from typing import List
from typing import Tuple
from typing import TYPE_CHECKING

import numpy as np

from ..stream import Stream

if TYPE_CHECKING:
    StreamValue = OrderedDict[bytes, bytes]
else:
    StreamValue = OrderedDict


class MovingAverageState:
    def __init__(self, windows: List[Tuple[str, int]]) -> None:
        self.windows = windows
        self.state = {
            field.encode(): np.array([np.nan] * window)
            for field, window in self.windows.items()
        }

    def _update(self, new_value: StreamValue) -> Dict[bytes, float]:
        for field, value in new_value.items():
            try:
                self.state[field][:-1] = self.state[field][1:]
                self.state[field][-1:] = float(value)
            except KeyError:
                pass

        output = {field: np.nanmean(values) for field, values in self.state.items()}

        # remove nan values
        for f in list(output):
            if np.isnan(output[f]):
                del output[f]

        # append value of fields not in window
        for f, v in new_value.items():
            if f not in self.state.keys():
                output[f] = float(v)

        return output

    def update(self, new_record: Tuple[bytes, bytes, StreamValue]):
        name, idx, new_value = new_record
        new_name = f"moving_average({name.decode()})".encode()
        self.new_output = self._update(new_value)
        return new_name, idx, self.new_output


class MovingAverage:
    def __init__(
        self, stream: Stream, window: Union[Tuple[str, int], List[Tuple[str, int]]]
    ) -> None:
        self.stream = stream

        if isinstance(window, tuple):
            self.windows = {window[0]: window[1]}
        elif isinstance(window, list):
            self.windows = {w[0]: w[1] for w in window}
        else:  # TODO: when if a list of other than tuples
            raise TypeError("MovingAverage window must be tuple or list of tuples.")

        self.state = MovingAverageState(self.windows)

        self.queue = asyncio.Queue()
        asyncio.ensure_future(self.stream._read(self.queue))

    @property
    def source_name(self) -> str:
        return self.stream.name

    @property
    def node_name(self) -> str:
        args = ", ".join([f"({k}, {v})" for k, v in self.windows.items()])
        node_name = f"moving_average({self.source_name})[{args}]"
        return node_name

    def __aiter__(self) -> MovingAverage:
        return self

    async def __anext__(self):
        res = await self.queue.get()
        return self.state.update(res)
