import pytest

from stream_tools import Stream


def test_stream_init():
    stream = Stream('test')
    assert stream.name == 'test'


@pytest.mark.asyncio
async def test_stream_context(redis):
    async with Stream('test') as stream:
        value = stream.read()
        redis.xadd('test', {"val": 1})
        a = await value.__anext__()
        assert a == ('test', {"val": 1})
