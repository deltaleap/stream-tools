import asyncio
from pprint import pprint

import uvloop

from stream_tools import App


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@app.join(["test_stream_1", "test_stream_2"], {mode: "update_state"})
async def func() -> None:
    print("===")
    ppring(value)
    print("===\n")


app.run(debug=True)
