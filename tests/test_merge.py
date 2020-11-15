import asyncio

import pytest

from typing import List
from typing import Dict
from typing import Tuple

from stream_tools import Stream
from stream_tools import Streams


@pytest.mark.asyncio
async def test_merge(redis) -> None:
    async def _main() -> List[Tuple[bytes, bytes, Dict[bytes, bytes]]]:
        stream1 = Stream('test_stream_merge_1')
        stream2 = Stream('test_stream_merge_2')
        async with Streams([stream1, stream2]) as streams:
            i = 0
            result = []
            async for value in streams.merge():
                if i < 5:
                    result.append(value)
                    i += 1
                else:
                    break
        return result

    async def _checker() -> List[bytes]:
        result = []
        for i in range(3):
            await asyncio.sleep(.1)
            val = await redis.xadd('test_stream_merge_1', {'x': float(i)})
            result.append(val)
            await asyncio.sleep(.1)
            val = await redis.xadd('test_stream_merge_2', {'x': (i + 1.0) * 2})
            result.append(val)
        return result

    check, res = await asyncio.gather(_checker(), _main())

    for idx, row in enumerate(res):
        assert row[1] == check[idx]

    for idx, row in enumerate(res):
        if idx % 2 == 0:
            assert row[0] == b'test_stream_merge_1'
        else:
            assert row[0] == b'test_stream_merge_2'

    assert res[0][2] == {b'x': str(float(0)).encode()}
    assert res[1][2] == {b'x': str(float((0 + 1.0) * 2)).encode()}
    assert res[2][2] == {b'x': str(float(1)).encode()}
    assert res[3][2] == {b'x': str(float((1 + 1.0) * 2)).encode()}
    assert res[4][2] == {b'x': str(float(2)).encode()}
