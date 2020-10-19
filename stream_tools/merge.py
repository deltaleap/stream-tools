import asyncio


class Merge:
    def __init__(self, redis, callback):
        self.redis = redis
        self.callback = callback
        self.queue = asyncio.Queue()
        asyncio.ensure_future(self.callback(self.queue))

    def __aiter__(self):
        return self

    async def __anext__(self):
        res = await self.queue.get()
        return res
