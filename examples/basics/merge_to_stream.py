import asyncio

import uvloop

from stream_tools import App


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

app = App(decode_responses=True)


@app.merge(["test_stream_1", "test_stream_2", "test_stream_4"])
@app.to("merged_stream")
async def merge(stream, value) -> Dict[str, str]:
    print(f"Processing new value from {stream}")

    if stream == "test_stream_1":
        res = value[1]['val'] * 100
    elif stream == "test_stream_2":
        res = value[1]['val'] * 200
    else:
        res = 'unknown'

    return {
        "sender": stream,
        "raw_value": value[1]['val'],
        "value": res,
    }


@app.agent("merged_stream")
async def func(value) -> None:
    print(f"{value[0]}: {value[1]['val']}")


app.run(debug=True)
