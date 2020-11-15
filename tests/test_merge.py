

def test_merge(redis) -> None:
    async def _main():
        stream1 = Stream('test_stream_1')
        stream2 = Stream('test_stream_2')
        async with Streams([stream1, stream2]) as streams:
            async for value in streams.merge():
                i = 0
                result = []

                while i < 6:
                    result.append(value)
                    i += 1

                return result

    async def _checker():
        await asyncio.sleep(.1)

