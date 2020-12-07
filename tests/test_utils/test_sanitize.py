from collections import namedtuple
from collections import OrderedDict

import pytest

from stream_tools import sanitize



@pytest.mark.parametrize(
    "test_input,expected",
    [
        ({b"x": b"1.2"}, {"x": b"1.2"}),
        ({b"x": b"1.2", b"y": b"3"}, {"x": b"1.2", "y": b"3"}),
        (
            {b"x": b"1.2", b"y": b"3", b"z": b"bid"},
            {"x": b"1.2", "y": b"3", "z": b"bid"}
        ),
        (
            OrderedDict(
                [
                    (b'x', b'1607350062.554'),
                    (b'y', b'3.680'),
                    (b'z', b'5.7335'),
                ]
            ),
            {"x": b"1607350062.554", "y": b"3.680", "z": b"5.7335"}
        )
    ]
)
def test_sanitize_keys(test_input, expected) -> None:
    assert sanitize(test_input, "keys") == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ({b"x": b"1.2"}, {b"x": 1.2}),
        ({b"x": b"1.2", b"y": b"3"}, {b"x": 1.2, b"y": 3}),
        (
            {b"x": b"1.2", b"y": b"3", b"z": b"bid"},
            {b"x": 1.2, b"y": 3, b"z": "bid"}
        ),
        (
            OrderedDict(
                [
                    (b'x', b'1607350062.554'),
                    (b'y', b'3.680'),
                    (b'z', b'5.7335'),
                ]
            ),
            {b"x": 1607350062.554, b"y": 3.680, b"z": 5.7335}
        )
    ]
)
def test_sanitize_values(test_input, expected) -> None:
    assert sanitize(test_input, "values") == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ({b"x": b"1.2"}, {"x": 1.2}),
        ({b"x": b"1.2", b"y": b"3"}, {"x": 1.2, "y": 3}),
        (
            {b"x": b"1.2", b"y": b"3", b"z": b"bid"},
            {"x": 1.2, "y": 3, "z": "bid"}
        ),
        (
            OrderedDict(
                [
                    (b'x', b'1607350062.554'),
                    (b'y', b'3.680'),
                    (b'z', b'5.7335'),
                ]
            ),
            {"x": 1607350062.554, "y": 3.680, "z": 5.7335}
        )
    ]
)
def test_sanitize_all(test_input, expected) -> None:
    assert sanitize(test_input, "all") == expected


datapoint = namedtuple("datapoint", "x y z")
@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            {b"x": b"1.2", b"y": b"1.6", b"z": b"91.0"},
            (1.2, 1.6, 91.0)
        ),
        (
            {b"x": b"a", b"y": b"b", b"z": b"c"},
            ("a", "b", "c")
        ),
        (
            {b"x": b"1.2", b"y": b"3", b"z": b"bid"},
            (1.2, 3, "bid")
        ),
        (
            OrderedDict(
                [
                    (b'x', b'1607350062.554'),
                    (b'y', b'3.680'),
                    (b'z', b'5.7335'),
                ]
            ),
            (1607350062.554, 3.680, 5.7335)
        )

    ]
)
def test_namedtuple_sanitize(test_input, expected):
    assert sanitize(test_input, datapoint) == datapoint(*expected)


#  TODO: sanitize to dataclass
"""
def test_dataclass_sanitize():
    pass
"""
