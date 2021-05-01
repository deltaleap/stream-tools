import asyncio
from pprint import pprint

import uvloop

from stream_tools import App


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

app = App()


@app.join(
    ['test_stream_1', 'test_stream_2'],
    {mode: "time_catch", timeframe: 2}
)
async def func(value) -> None:
    print("===")
    pprint(value)
    print("===")


app.run(debug=True)
