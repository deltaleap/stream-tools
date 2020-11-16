from stream_tools import Stream
from stream_tools import Streams


def test_streams_init() -> None:
    stream_a = Stream("a")
    stream_b = Stream("b")
    streams = Streams([stream_a, stream_b])

    assert streams.stream_list == [stream_a, stream_b]
    assert streams.stream_names == ["a", "b"]
