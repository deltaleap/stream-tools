from __future__ import annotations

import asyncio

from collections import OrderedDict
from typing import Union
from typing import Dict
from typing import List
from typing import Tuple
from typing import TYPE_CHECKING

import numpy as np  # type: ignore

from ..stream import Stream

if TYPE_CHECKING:
    StreamValue = OrderedDict[bytes, bytes]
    StreamRecord = Tuple[bytes, bytes, StreamValue]
    StreamQueue = asyncio.Queue[StreamRecord]
else:
    StreamValue = OrderedDict
    StreamQueue = asyncio.Queue


class MovingAverageState:
    """State class to manage moving average data and results
    """
    def __init__(self, windows: Dict[str, int]) -> None:
        """Initialize the Moving Average State class

        Args:
            windows (Dict[str, int]): the moving average window for each
                field
        """
        self.windows = windows
        self.state = {
            field.encode(): np.array([np.nan] * window)
            for field, window in self.windows.items()
        }

    def _update(self, new_value: StreamValue) -> Dict[bytes, float]:
        """Calculate the moving average with the new values, store new_values
            and return the output

        Args:
            new_value (StreamValue): the output of the moving average
                calculated with the new value

        Returns:
            Dict[bytes, float]: the result of the moving average for each
                of the fields of the new value provided
        """
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

    def update(
        self, new_record: Tuple[bytes, bytes, StreamValue]
    ) -> Tuple[bytes, bytes, Dict[bytes, float]]:
        """Update the state of the moving average and return the result

        Args:
            new_record (Tuple[bytes, bytes, StreamValue]): the new value to
                calculate the moving average

        Returns:
            Tuple[bytes, bytes, Dict[bytes, float]]: the new moving average
                result as a tuple: (name of the moving average node, redis id
                of the new observation provided, the dictionary containing for
                each field the result of the moving average)
        """
        name, idx, new_value = new_record
        new_name = f"moving_average({name.decode()})".encode()
        self.new_output = self._update(new_value)
        return new_name, idx, self.new_output


class MovingAverage:
    """Moving Average Filter class. Calculate the moving average of the
        provided streams. This filter can calculated a moving average
        with different time windows for each fields in the stream.
    """
    def __init__(
        self, stream: Stream, window: Union[Tuple[str, int], List[Tuple[str, int]]]
    ) -> None:
        """Initialize the moving average filter and start the reader function

        Args:
            stream (Stream): the source stream
            window (Union[Tuple[str, int], List[Tuple[str, int]]]): moving
                average window for each field of the provieded source
                stream

        Raises:
            TypeError: in case of wrong window type
        """
        self.stream = stream

        if isinstance(window, tuple):
            self.windows = {window[0]: window[1]}
        elif isinstance(window, list):
            self.windows = {w[0]: w[1] for w in window}
        else:  # TODO: when if a list of other than tuples
            raise TypeError("MovingAverage window must be tuple or list of tuples.")

        self.state = MovingAverageState(self.windows)

        self.queue: StreamQueue = asyncio.Queue()
        asyncio.ensure_future(self.stream._read(self.queue))

    @property
    def source_name(self) -> str:
        """The source stream name getter

        Returns:
            str: the name of the source streams
        """
        return self.stream.name

    @property
    def node_name(self) -> str:
        """The node name getter

        Returns:
            str: the node name
        """
        args = ", ".join([f"({k}, {v})" for k, v in self.windows.items()])
        node_name = f"moving_average({self.source_name})[{args}]"
        return node_name

    def __aiter__(self) -> MovingAverage:
        """Get the moving average iterator

        Returns:
            MovingAverage: the moving average instance
        """
        return self

    async def __anext__(self) -> Tuple[bytes, bytes, Dict[bytes, float]]:
        """Get the next value from the queue and update
            the state of the Moving Average

        Returns:
            Tuple[bytes, bytes, Dict[bytes, float]]: the new state of the
                Moving Average (provided by the Moving Average State class)
        """
        res = await self.queue.get()
        return self.state.update(res)
