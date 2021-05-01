# Stream Tools

`stream-tools` is a python module to easily work with Redis Streams.
`stream-tools` aims to help building scripts to read, transform and write data from and to streams.
The syntax is inspired by [pallets/flask](https://github.com/pallets/flask) and the modulo want to be a lighter Redis Stream version of [robinhood/faust](https://github.com/robinhood/faust).

`stream-tools` want to help developing stream nodes with a syntax simple and similar to the `flask.route` one:

```python
from stream_tools import App

app = App(decode_responses=True)

@app.agent("stream_1")
async def print_data(value) -> None:
    print(f"id: {value[0]}")
    print(f"content: {value[1]}\n")


app.run()
```


### How to

To start learning how to use `stream-tools` you have the following alternatives:

* read this documentation
* check the [examples folder](https://github.com/deltaleap/stream-tools/tree/main/examples)
