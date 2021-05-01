import asyncio

from stream_tools import App


app = App(decode_responses=True)


@app.agent("stream_1")
async def print_data(value) -> None:
    print(f"id: {value[0]}")
    print(f"content: {value[1]}\n")


app.run()
