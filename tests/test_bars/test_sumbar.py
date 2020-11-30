import asyncio

from collections import OrderedDict
from typing import List

import pytest

from stream_tools import Stream
from stream_tools.bars import SumBar
from stream_tools.bars.sumbar import SumBarState


def test_sumbar_state_one_arg_one_value() -> None:
    sb_state = SumBarState({"x": 10.0})
    sb_state.update((b"a", b"1606081071444-0", OrderedDict({b"x": b"5.0"})))
    assert sb_state.trigger is False
    sb_state.update((b"a", b"1606081071587-0", OrderedDict({b"x": b"6.0"})))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081071587-0",
        {b"x": 11.0}
    )
    sb_state.update((b"a", b"1606081071619-0", OrderedDict({b"x": b"3.0"})))
    assert sb_state.trigger is False
    sb_state.update((b"a", b"1606081071991-0", OrderedDict({b"x": b"3.0"})))
    assert sb_state.trigger is False
    sb_state.update((b"a", b"1606081072351-0", OrderedDict({b"x": b"16.0"})))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081072351-0",
        {b"x": 22.0}
    )


def test_sumbar_state_one_arg_two_values() -> None:
    sb_state = SumBarState({"x": 10.0})
    sb_state.update((
        b"a",
        b"1606081071444-0",
        OrderedDict({b"x": b"5.0", b"y": b"12.0"})
    ))
    assert sb_state.trigger is False
    sb_state.update((
        b"a",
        b"1606081071587-0",
        OrderedDict({b"x": b"6.0", b"y": b"18.0"})
    ))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081071587-0",
        {b"x": 11.0, b"y": 30.0}
    )
    sb_state.update((
        b"a",
        b"1606081071619-0",
        OrderedDict({b"x": b"3.0", b"y": b"22.0"})
    ))
    assert sb_state.trigger is False
    sb_state.update((
        b"a",
        b"1606081071991-0",
        OrderedDict({b"x": b"3.0", b"y": b"15.0"})
    ))
    assert sb_state.trigger is False
    sb_state.update((
        b"a",
        b"1606081072351-0",
        OrderedDict({b"x": b"16.0", b"y": b"9.0"})
    ))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081072351-0",
        {b"x": 22.0, b"y": 46.0}
    )


def test_sumbar_state_two_args_two_values() -> None:
    sb_state = SumBarState({"x": 10.0, "y": 20.0})
    sb_state.update((
        b"a",
        b"1606081071444-0",
        OrderedDict({b"x": b"5.0", b"y": b"12.0"})
    ))
    assert sb_state.trigger is False
    sb_state.update((
        b"a",
        b"1606081071587-0",
        OrderedDict({b"x": b"6.0", b"y": b"18.0"})
    ))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081071587-0",
        {b"x": 11.0, b"y": 30.0}
    )
    sb_state.update((
        b"a",
        b"1606081071619-0",
        OrderedDict({b"x": b"3.0", b"y": b"22.0"})
    ))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081071619-0",
        {b"x": 3.0, b"y": 22.0}
    )
    sb_state.update((
        b"a",
        b"1606081071991-0",
        OrderedDict({b"x": b"3.0", b"y": b"15.0"})
    ))
    assert sb_state.trigger is False
    sb_state.update((
        b"a",
        b"1606081072351-0",
        OrderedDict({b"x": b"16.0", b"y": b"9.0"})
    ))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081072351-0",
        {b"x": 19.0, b"y": 24.0}
    )


def test_sumbar_state_two_args_one_value() -> None:
    sb_state = SumBarState({"x": 10.0, "y": 25.0})
    sb_state.update((b"a", b"1606081071444-0", OrderedDict({b"x": b"5.0"})))
    assert sb_state.trigger is False
    sb_state.update((b"a", b"1606081071587-0", OrderedDict({b"x": b"6.0"})))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081071587-0",
        {b"x": 11.0, b"y": 0.0}
    )
    sb_state.update((b"a", b"1606081071619-0", OrderedDict({b"x": b"3.0"})))
    assert sb_state.trigger is False
    sb_state.update((b"a", b"1606081071991-0", OrderedDict({b"x": b"3.0"})))
    assert sb_state.trigger is False
    sb_state.update((b"a", b"1606081072351-0", OrderedDict({b"x": b"16.0"})))
    assert sb_state.trigger is True
    assert sb_state.output == (
        b"sum(a)",
        b"1606081072351-0",
        {b"x": 22.0, b"y": 0.0}
    )


def test_sumbar_init() -> None:
    stream1 = Stream("test_stream")
    stream2 = Stream("another_test_stream")
    sb1 = SumBar(stream1, ("x", 5))
    sb2 = SumBar(stream2, [("x", 5), ("y", 2)])

    assert sb1.source_name == "test_stream"
    assert sb1.node_name == "sum_bar(test_stream)[(x, 5)]"
    assert sb2.source_name == "another_test_stream"
    assert sb2.node_name == "sum_bar(another_test_stream)[(x, 5), (y, 2)]"


def test_sumbar_with_wrong_init_args() -> None:
    stream = Stream("test_stream")
    with pytest.raises(TypeError):
        SumBar(stream)
    with pytest.raises(TypeError):
        SumBar(stream, 5)
    with pytest.raises(TypeError):
        SumBar(stream, "x")
    with pytest.raises(TypeError):
        SumBar(stream, {"x": 5})


@pytest.mark.asyncio
async def test_sumbar_with_one_arg_one_field(redis) -> None:
    async def _main():
        stream = Stream("test_stream")
        async with stream:
            i = 0
            data_result = []
            idxs = []
            async for value in SumBar(stream, ("x", 5)):
                if i < 2:
                    data_result.append(dict(value[2]))
                    idxs.append(value[1])
                    i += 1
                else:
                    break
            return data_result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(0.1)
        # time: 0.1
        a = await redis.xadd("test_stream", {"x": 2.0})
        # time: 0.3
        await asyncio.sleep(0.3)
        b = await redis.xadd("test_stream", {"x": 1.0})
        # time: 0.9
        await asyncio.sleep(0.9)
        c = await redis.xadd("test_stream", {"x": 3.0})
        # time: 1.3
        await asyncio.sleep(1.3)
        d = await redis.xadd("test_stream", {"x": 6.0})
        # time: 1.8
        await asyncio.sleep(1.8)
        e = await redis.xadd("test_stream", {"x": 2.0})
        # time: 2.1
        await asyncio.sleep(2.1)
        f = await redis.xadd("test_stream", {"x": 1.0})
        await asyncio.sleep(0.1)
        await redis.xadd("test_stream", {"x": 100})
        return [a, b, c, d, e, f]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b"x": 6.0},
        {b"x": 6.0}
    ]

    assert expected_res == res


@pytest.mark.asyncio
async def test_sum_with_one_arg_two_field(redis) -> None:
    async def _main():
        stream = Stream("test_stream")
        async with stream:
            i = 0
            data_result = []
            idxs = []
            async for value in SumBar(stream, ("x", 5)):
                if i < 2:
                    data_result.append(dict(value[2]))
                    idxs.append(value[1])
                    i += 1
                else:
                    break
            return data_result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(0.1)
        # time: 0.1
        a = await redis.xadd("test_stream", {"x": 2.0, "y": 3.0})
        # time: 0.3
        await asyncio.sleep(0.3)
        b = await redis.xadd("test_stream", {"x": 1.0, "y": 7.0})
        # time: 0.9
        await asyncio.sleep(0.9)
        c = await redis.xadd("test_stream", {"x": 3.0, "y": 4.0})
        # time: 1.3
        await asyncio.sleep(1.3)
        d = await redis.xadd("test_stream", {"x": 6.0, "y": 13.0})
        # time: 1.8
        await asyncio.sleep(1.8)
        e = await redis.xadd("test_stream", {"x": 2.0, "y": 1.0})
        # time: 2.1
        await asyncio.sleep(2.1)
        f = await redis.xadd("test_stream", {"x": 1.0, "y": 9.0})
        await asyncio.sleep(0.1)
        await redis.xadd("test_stream", {"x": 100.0})
        return [a, b, c, d, e, f]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b"x": 6.0, b"y": 14.0},
        {b"x": 6.0, b"y": 13.0}
    ]

    assert expected_res == res


@pytest.mark.asyncio
async def test_sum_with_two_args_two_fields(redis) -> None:
    async def _main():
        stream = Stream("test_stream")
        async with stream:
            i = 0
            data_result = []
            idxs = []
            async for value in SumBar(stream, [("x", 5), ("y", 10)]):
                if i < 3:
                    data_result.append(dict(value[2]))
                    idxs.append(value[1])
                    i += 1
                else:
                    break
            return data_result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(0.1)
        # time: 0.1
        a = await redis.xadd("test_stream", {"x": 2.0, "y": 3.0})
        # time: 0.3
        await asyncio.sleep(0.3)
        b = await redis.xadd("test_stream", {"x": 1.0, "y": 7.0})
        # time: 0.9
        await asyncio.sleep(0.9)
        c = await redis.xadd("test_stream", {"x": 3.0, "y": 4.0})
        # time: 1.3
        await asyncio.sleep(1.3)
        d = await redis.xadd("test_stream", {"x": 6.0, "y": 13.0})
        # time: 1.8
        await asyncio.sleep(1.8)
        e = await redis.xadd("test_stream", {"x": 2.0, "y": 1.0})
        # time: 2.1
        await asyncio.sleep(2.1)
        f = await redis.xadd("test_stream", {"x": 1.0, "y": 9.0})
        await asyncio.sleep(2)
        await redis.xadd("test_stream", {"x": 100.0})
        return [a, b, c, d, e, f]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b"x": 3.0, b"y": 10.0},
        {b"x": 9.0, b"y": 17.0},
        {b"x": 3.0, b"y": 10.0}
    ]

    assert expected_res == res


@pytest.mark.asyncio
async def test_sum_with_two_arg_one_field(redis) -> None:
    async def _main():
        stream = Stream("test_stream")
        async with stream:
            i = 0
            data_result = []
            idxs = []
            async for value in SumBar(stream, [("x", 5), ("y", 10)]):
                if i < 2:
                    data_result.append(dict(value[2]))
                    idxs.append(value[1])
                    i += 1
                else:
                    break
            return data_result

    async def _checker() -> List[bytes]:
        await asyncio.sleep(0.1)
        # time: 0.1
        a = await redis.xadd("test_stream", {"x": 2.0})
        # time: 0.3
        await asyncio.sleep(0.3)
        b = await redis.xadd("test_stream", {"x": 1.0})
        # time: 0.9
        await asyncio.sleep(0.9)
        c = await redis.xadd("test_stream", {"x": 3.0})
        # time: 1.3
        await asyncio.sleep(1.3)
        d = await redis.xadd("test_stream", {"x": 6.0})
        # time: 1.8
        await asyncio.sleep(1.8)
        e = await redis.xadd("test_stream", {"x": 2.0})
        # time: 2.1
        await asyncio.sleep(2.1)
        f = await redis.xadd("test_stream", {"x": 1.0})
        await asyncio.sleep(0.1)
        await redis.xadd("test_stream", {"x": 100})
        return [a, b, c, d, e, f]

    check, res = await asyncio.gather(_checker(), _main())

    expected_res = [
        {b"x": 6.0, b"y": 0.0},
        {b"x": 6.0, b"y": 0.0}
    ]

    assert expected_res == res
