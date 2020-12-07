from collections import namedtuple

from typing import Dict
from typing import Union
from typing import NamedTuple


def sanitize(
    input_data: Dict[bytes, bytes],
    mode: str = "all"
) -> Dict[Union[str, bytes], Union[int, float, str, bytes]]:
    if mode == "keys":
        return _sanitize_keys(input_data)
    elif mode == "values":
        return _sanitize_values(input_data)
    elif mode == "all":
        return _sanitize_all(input_data)
    else:
        if "_fields" in mode.__dict__:  # is a namedtuple
            return _sanitize_to_namedtuple(input_data, mode)
        else:
            raise ValueError('Provided schema is not supported.')


def _sanitize_keys(data: Dict[bytes, bytes]) -> Dict[str, bytes]:
    return {k.decode(): v for k, v in data.items()}


def _sanitize_values(data: Dict[bytes, bytes]) -> Dict[bytes, Union[int, float, str]]:
    new_data = {}
    for k, v in data.items():
        try:
            new_value = int(v)
        except ValueError:
            try:
                new_value = float(v)
            except ValueError:
                new_value = v.decode()
        new_data[k] = new_value
    return new_data


def _sanitize_all(data: Dict[bytes, bytes]) -> Dict[str, Union[int, float, str]]:
    return _sanitize_keys(
        _sanitize_values(
            data
        )
    )


def _sanitize_to_namedtuple(data: Dict[bytes, bytes], nt: NamedTuple) -> NamedTuple:
    new_data = _sanitize_all(data)

    for f in nt._fields:
        try:
            new_data[f]
        except KeyError:
            raise KeyError("Namedtuple fields not compatible with provided data")

    return nt(**new_data)
