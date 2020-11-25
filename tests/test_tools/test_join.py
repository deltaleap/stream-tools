import asyncio

from typing import List
from typing import Dict

import pytest

from stream_tools import Stream
from stream_tools import Streams


@pytest.mark.asyncio
async def test_join_file_without_join_method(redis) -> None:
    async def _main() -> None:
        stream1 = Stream("test_stream_join_1")
        stream2 = Stream("test_stream_join_2")

        async with Streams([stream1, stream2]) as streams:
            async for value in streams.join():
                print(value)

    with pytest.raises(TypeError):
        await _main()


@pytest.mark.asyncio
async def test_join_time_catch_with_no_time_value(redis) -> None:
    async def _main() -> None:
        stream1 = Stream("test_stream_1")
        stream2 = Stream("test_stream_2")

        async with Streams([stream1, stream2]) as streams:
            async for value in streams.join("time_catch"):
                return value

        with pytest.raises(TypeError):
            await _main()


@pytest.mark.asyncio
async def test_join_update_state(redis) -> None:
    async def _main() -> List[Dict[bytes, Dict[bytes, bytes]]]:
        stream1 = Stream("test_stream_1")
        stream2 = Stream("test_stream_2")

        async with Streams([stream1, stream2]) as streams:
            i = 0
            result = []
            async for value in streams.join("update_state"):
                if i < 3:
                    # value: {b'stream_name': (
                    #     b'id', OrderedDict(b'k': b'v')
                    # )}
                    result.append({k: dict(v[1]) for k, v in value.items()})
                    i += 1
                else:
                    break
            return result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(0.1)
        # time: 0.1
        a = await redis.xadd("test_stream_1", {"x": 1.0})
        # time: 0.2
        await asyncio.sleep(0.1)
        b = await redis.xadd("test_stream_2", {"x": 2.0})
        # time: 0.8
        await asyncio.sleep(0.6)
        c = await redis.xadd("test_stream_1", {"x": 5.0})
        await redis.xadd("test_stream_1", {"x": 1})  # just to stop the stream
        return [a, b, c]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b"test_stream_1": {b"x": b"1.0"}},
        {b"test_stream_1": {b"x": b"1.0"}, b"test_stream_2": {b"x": b"2.0"}},
        {b"test_stream_1": {b"x": b"5.0"}, b"test_stream_2": {b"x": b"2.0"}},
    ]

    assert [{k: v for k, v in val.items()} for val in res] == expected_res


@pytest.mark.asyncio
async def test_join_time_catch(redis) -> None:
    async def _main() -> List[Dict[bytes, Dict[bytes, bytes]]]:
        stream1 = Stream("test_stream_1")
        stream2 = Stream("test_stream_2")

        async with Streams([stream1, stream2]) as streams:
            i = 0
            result = []
            async for value in streams.join("time_catch", 0.3):
                if i < 5:
                    # value: {b'stream_name': (
                    #     b'id', OrderedDict(b'k': b'v')
                    # )}
                    result.append({k: dict(v[1]) for k, v in value.items()})
                    i += 1
                else:
                    break
            return result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(0.1)
        # time: 0.1
        a = await redis.xadd("test_stream_1", {"x": 1.0})
        # time: 0.2
        await asyncio.sleep(0.1)
        b = await redis.xadd("test_stream_2", {"x": 2.0})
        # time: 0.8
        await asyncio.sleep(0.6)
        c = await redis.xadd("test_stream_1", {"x": 5.0})
        # time: 1.0
        await asyncio.sleep(0.2)
        d = await redis.xadd("test_stream_2", {"x": 3.0})
        # time: 1.5
        await asyncio.sleep(0.5)
        e = await redis.xadd("test_stream_2", {"x": 9.0})
        await redis.xadd("test_stream_1", {"x": 1})  # just to stop the stream
        return [a, b, c, d, e]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b"test_stream_1": {b"x": b"1.0"}},
        {b"test_stream_1": {b"x": b"1.0"}, b"test_stream_2": {b"x": b"2.0"}},
        {
            b"test_stream_1": {b"x": b"5.0"},
        },
        {b"test_stream_1": {b"x": b"5.0"}, b"test_stream_2": {b"x": b"3.0"}},
        {b"test_stream_2": {b"x": b"9.0"}},
    ]

    assert [{k: v for k, v in val.items()} for val in res] == expected_res
