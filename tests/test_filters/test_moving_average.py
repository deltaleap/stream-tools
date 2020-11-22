import asyncio

import pytest

from stream_tools import Stream
from stream_tools.filters import MovingAverage


def test_moving_average_init() -> None:
    stream = Stream('test_stream')
    ma = MovingAverage(stream, ('x', 5))

    assert ma.source_name == 'test_stream'
    assert ma.node_name == 'moving_average(test_stream)[(x, 5)]'


@pytest.mark.asyncio
async def test_moving_average_with_no_arg() -> None:
    async def _main() -> None:
        stream = Stream('test_stream')
        async with stream:
            async for value in MovingAverage(stream):
                print(value)

    
    with pytest.raises(TypeError):  # No (field, window) passed
        await _main()


@pytest.mark.asyncio
async def test_moving_average_with_one_arg(redis) -> None:
    async def _main():
        stream = Stream('test_stream')
        async with stream:
            i = 0
            result = []
            async for value in MovingAverage(stream, ('x', 3)):
                if i < 5:
                    result.append(dict(value[2]))
                    i += i
                else:
                    break
            return result

    async def _check() -> List[bytes]:
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
        {'x': 1.0, 'y': 10.0},
        {'x': 2.0, 'y': 5.6},
        {'x': 5.7, 'y': 3.0},
        {'x': 6.7, 'y': 9.0},
    ]

    assert expected_res == res


@pytest.mark.asyncio
async def test_moving_average_with_two_args(redis) -> None:
    async def _main():
        stream = Stream('test_stream')
        async with stream:
            i = 0
            result = []
            async for value in MovingAverage(stream, ('x', 3), ('y', 2)):
                if i < 5:
                    result.append(dict(value[2]))
                    i += i
                else:
                    break
            return result

    async def _check() -> List[bytes]:
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
        {'x': 1.0, 'y': 10.0},
        {'x': 2.0, 'y': 7.8},
        {'x': 5.7, 'y': 4.3},
        {'x': 6.7, 'y': 6.0},
    ]

    assert expected_res == res


@pytest.mark.asyncio
async def test_moving_average_with_missing_field(redis) -> None:
    async def _main():
        stream = Stream('test_stream')
        async with stream:
            i = 0
            result = []
            async for value in MovingAverage(stream, ('x', 3), ('z', 2)):
                if i < 5:
                    result.append(dict(value[2]))
                    i += i
                else:
                    break
            return result

    async def _check() -> List[bytes]:
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
        {'x': 1.0, 'y': 10.0},
        {'x': 2.0, 'y': 5.6},
        {'x': 5.7, 'y': 3.0},
        {'x': 6.7, 'y': 9.0},
    ]

    assert expected_res == res
