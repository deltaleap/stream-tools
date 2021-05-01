import asyncio

from stream_tools import App


app = App(decode_responses=True)


@app.agent("stream_1")
@app.to("new_stream")
async def calculation(value) -> Dict[str, str]:
    # receiving a value
    print(f"stream_1: {value[0]}")
    # calculating the new value
    new_value = value[1]["val"] * 10 + 5

    # wait some time...
    await asyncio.sleep(3)

    # return the record to push to the stream
    return {"new_val": new_value}


# now observe the new stream
@app.agent("new_stream")
async def print_new_stream(value) -> None:
    print(f"new_stream: {value[0]}")
    print(f"content: {value[1]}")


app.run()
