import asyncio

import uvloop

from stream_tools import App


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

app = App(decode_responses=True)


@app.merge(["test_stream_1", "test_stream_2", "test_stream_4"])
async def func(stream, value) -> None:
    # Print a tuple:
    #  (stream name, record id, record content)
    print(f"{stream} -> {value[0]}: {value[1]['val']}")


app.run(debug=True)
