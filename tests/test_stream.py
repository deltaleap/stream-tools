from __future__ import annotations
import asyncio

from collections import OrderedDict
from typing import Tuple
from typing import Dict
from typing import List
from typing import TYPE_CHECKING

import aioredis
import pytest  # type: ignore

from stream_tools import Stream

if TYPE_CHECKING:
    StreamValue = OrderedDict[bytes, bytes]
else:
    StopValue = OrderedDict


def test_stream_init() -> None:
    stream = Stream("test")
    assert stream.name == "test"


@pytest.mark.asyncio
async def test_read_one_record(redis: aioredis.Redis) -> None:
    async def _main() -> Tuple[bytes, bytes, Dict[bytes, bytes]]:
        async with Stream("test_stream_1") as s:
            async for value in s.read():
                result = value
                break
            return result

    async def _checker() -> bytes:
        await asyncio.sleep(0.1)
        val = await redis.xadd("test_stream_1", {"x": 10.0})
        return val

    res = await asyncio.gather(_checker(), _main())

    assert res[1][0] == b"test_stream_1"
    assert res[1][1] == res[0]
    assert res[1][2] == {b"x": b"10.0"}


@pytest.mark.asyncio
async def test_read_multiple_records(redis: aioredis.Redis) -> None:
    async def _main() -> List[Tuple[bytes, bytes, StreamValue]]:
        async with Stream("test_stream_1") as stream:
            i = 0
            result = []
            async for value in stream.read():
                if i < 5:
                    result.append(value)
                    i += 1
                else:
                    break
        return result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(0.1)
        result = []
        for i in range(6):
            val = await redis.xadd("test_stream_1", {"x": i})
            await asyncio.sleep(0.1)
            result.append(val)
        return result

    check, res = await asyncio.gather(_checker(), _main())
    for row in res:
        assert row[0] == b"test_stream_1"

    for idx, row in enumerate(res):
        assert row[1] == check[idx]

    for idx, row in enumerate(res):
        assert row[2] == {b"x": str(idx).encode()}
