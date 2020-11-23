import asyncio

from collections import OrderedDict
from typing import List

import numpy as np
import pytest

from stream_tools import Stream
from stream_tools.filters import MovingAverage
from stream_tools.filters.moving_average import MovingAverageState


def test_movave_state_one_arg_one_value() -> None:
    ma_state = MovingAverageState({'x': 3})
    assert ma_state.update(
        (b'a', b'1606081071444-0', OrderedDict({b'x': b'1.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071444-0',
        {b'x': 1.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071587-0', OrderedDict({b'x': b'2.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071587-0',
        {b'x': 1.5}
    )
    assert ma_state.update(
        (b'a', b'1606081071619-0', OrderedDict({b'x': b'3.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071619-0',
        {b'x': 2.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071991-0', OrderedDict({b'x': b'4.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071991-0',
        {b'x': 3.0}
    )


def test_movave_state_one_arg_two_values() -> None:
    ma_state = MovingAverageState({'x': 3})
    assert ma_state.update(
        (b'a', b'1606081071444-0', OrderedDict({b'x': b'1.0', b'y': b'11.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071444-0',
        {b'x': 1.0, b'y': 11.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071587-0', OrderedDict({b'x': b'2.0', b'y': b'19.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071587-0',
        {b'x': 1.5, b'y': 19.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071619-0', OrderedDict({b'x': b'3.0', b'y': b'21.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071619-0',
        {b'x': 2.0, b'y': 21.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071991-0', OrderedDict({b'x': b'4.0', b'y': b'31.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071991-0',
        {b'x': 3.0, b'y': 31.0}
    )


def test_movave_state_two_args_two_values() -> None:
    ma_state = MovingAverageState({'x': 3, 'y': 2})
    assert ma_state.update(
        (b'a', b'1606081071444-0', OrderedDict({b'x': b'1.0', b'y': b'11.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071444-0',
        {b'x': 1.0, b'y': 11.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071587-0', OrderedDict({b'x': b'2.0', b'y': b'19.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071587-0',
        {b'x': 1.5, b'y': 15.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071619-0', {b'x': b'3.0', b'y': b'21.0'})
    ) == (
        b'moving_average(a)',
        b'1606081071619-0',
        {b'x': 2.0, b'y': 20.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071991-0', {b'x': b'4.0', b'y': b'31.0'})
    ) == (
        b'moving_average(a)',
        b'1606081071991-0',
        {b'x': 3.0, b'y': 26.0}
    )


def test_movave_state_two_args_one_value() -> None:
    ma_state = MovingAverageState({'x': 3, 'y': 2})
    assert ma_state.update(
        (b'a', b'1606081071444-0', OrderedDict({b'x': b'1.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071444-0',
        {b'x': 1.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071587-0', OrderedDict({b'x': b'2.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071587-0',
        {b'x': 1.5}
    )
    assert ma_state.update(
        (b'a', b'1606081071619-0', OrderedDict({b'x': b'3.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071619-0',
        {b'x': 2.0}
    )
    assert ma_state.update(
        (b'a', b'1606081071991-0', OrderedDict({b'x': b'4.0'}))
    ) == (
        b'moving_average(a)',
        b'1606081071991-0',
        {b'x': 3.0}
    )


def test_moving_average_init() -> None:
    stream1 = Stream('test_stream')
    stream2 = Stream('another_test_stream')
    ma1 = MovingAverage(stream1, ('x', 5))
    ma2 = MovingAverage(
        stream2,
        [('x', 5), ('y', 2)]
    )

    assert ma1.source_name == 'test_stream'
    assert ma1.node_name == 'moving_average(test_stream)[(x, 5)]'
    assert ma2.source_name == 'another_test_stream'
    assert ma2.node_name == 'moving_average(another_test_stream)[(x, 5), (y, 2)]'


def test_moving_average_wrong_init_args() -> None:
    stream = Stream('test_stream')
    with pytest.raises(TypeError):
        ma = MovingAverage(stream)
    with pytest.raises(TypeError):
        ma = MovingAverage(stream, 5)
    with pytest.raises(TypeError):
        ma = MovingAverage(stream, 'x')
    with pytest.raises(TypeError):
        ma = MovingAverage(stream, {'x': 5})


@pytest.mark.asyncio
async def test_moving_average_with_one_arg(redis) -> None:
    async def _main():
        stream = Stream('test_stream')
        async with stream:
            i = 0
            result = []
            async for value in MovingAverage(stream, ('x', 3)):
                if i < 4:
                    print(value)
                    result.append(dict(value[2]))
                    i += 1
                else:
                    break
            return result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(.1)
        # time: 1
        a = await redis.xadd('test_stream', {'x': 1.0, 'y': 10.0})
        # time: 2
        await asyncio.sleep(2)
        b = await redis.xadd('test_stream', {'x': 3.0, 'y': 5.6})
        # time: 6
        await asyncio.sleep(4)
        c = await redis.xadd('test_stream', {'x': 13.1, 'y': 3.0})
        # time: 9
        await asyncio.sleep(3)
        d = await redis.xadd('test_stream', {'x': 4.0, 'y': 9.0})
        await redis.xadd('test_stream', {'x': 1})  # just to stop the stream
        return [a, b, c, d]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b'x': 1.0, b'y': 10.0},
        {b'x': 2.0, b'y': 5.6},
        {b'x': 5.7, b'y': 3.0},
        {b'x': 6.7, b'y': 9.0},
    ]

    assert expected_res == res


@pytest.mark.asyncio
async def test_moving_average_with_two_args(redis) -> None:
    async def _main():
        stream = Stream('test_stream')
        async with stream:
            i = 0
            result = []
            async for value in MovingAverage(stream, [('x', 3), ('y', 2)]):
                if i < 4:
                    result.append(dict(value[2]))
                    i += 1
                else:
                    break
            return result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(.1)
        # time: 1
        a = await redis.xadd('test_stream', {'x': 1.0, 'y': 10.0})
        # time: 2
        await asyncio.sleep(2)
        b = await redis.xadd('test_stream', {'x': 3.0, 'y': 5.6})
        # time: 6
        await asyncio.sleep(4)
        c = await redis.xadd('test_stream', {'x': 13.1, 'y': 3.0})
        # time: 9
        await asyncio.sleep(3)
        d = await redis.xadd('test_stream', {'x': 4.0, 'y': 9.0})
        await redis.xadd('test_stream', {'x': 1})  # just to stop the stream
        return [a, b, c, d]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b'x': 1.0, b'y': 10.0},
        {b'x': 2.0, b'y': 7.8},
        {b'x': 5.7, b'y': 4.3},
        {b'x': 6.7, b'y': 6.0},
    ]

    assert expected_res == res


@pytest.mark.asyncio
async def test_moving_average_with_missing_field(redis) -> None:
    async def _main():
        stream = Stream('test_stream')
        async with stream:
            i = 0
            result = []
            async for value in MovingAverage(stream, [('x', 3), ('z', 2)]):
                if i < 4:
                    result.append(dict(value[2]))
                    i += 1
                else:
                    break
            return result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(.1)
        # time: 1
        a = await redis.xadd('test_stream', {'x': 1.0, 'y': 10.0})
        # time: 2
        await asyncio.sleep(2)
        b = await redis.xadd('test_stream', {'x': 3.0, 'y': 5.6})
        # time: 6
        await asyncio.sleep(4)
        c = await redis.xadd('test_stream', {'x': 13.1, 'y': 3.0})
        # time: 9
        await asyncio.sleep(3)
        d = await redis.xadd('test_stream', {'x': 4.0, 'y': 9.0})
        await redis.xadd('test_stream', {'x': 1})  # just to stop the stream
        return [a, b, c, d]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b'x': 1.0, b'y': 10.0},
        {b'x': 2.0, b'y': 5.6},
        {b'x': 5.7, b'y': 3.0},
        {b'x': 6.7, b'y': 9.0},
    ]

    assert expected_res == res
