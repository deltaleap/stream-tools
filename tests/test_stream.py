import asyncio

import pytest

from stream_tools import Stream


def test_stream_init():
    stream = Stream('test')
    assert stream.name == 'test'


@pytest.mark.asyncio
async def test_read_one_record(redis):
    async def _main():
        async with Stream('test_stream_1') as s:
            async for value in s.read():
                return value

    async def _checker():
        await asyncio.sleep(.1)
        val = await redis.xadd('test_stream_1', {'x': 10.0})
        return val

    res = await asyncio.gather(_checker(), _main())

    assert res[1][0] == b'test_stream_1'
    assert res[1][1] == res[0]
    assert res[1][2] == {b'x': b'10.0'}


@pytest.mark.asyncio
async def test_read_multiple_records(redis):
    async def _main():
        async with Stream('test_stream_1') as s:
            async for value in s.read():
                i = 0
                result = []
                if i < 5:
                    result.append(value)
                    i += 1
                return result

    async def _checker():
        await asyncio.sleep(.1)
        result = []
        for i in range(5):
            val = await redis.xadd('test_stream_1', {'x': i})
            await asyncio.sleep(.1)
            result.append(val)
        return result

    res = await asyncio.gather(_checker(), _main())
    import pprint
    for row in res[1]:
        pprint.pprint(row)
        assert row[0] == b'test_stream_1'

    for idx, row in enumerate(res[1]):
        print(idx)
        assert row[1] == res[0][idx]

    for idx, row in enumerate(res[1]):
        assert row[2] == {b'x': str(idx).encode()}
