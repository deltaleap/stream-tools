import asyncio


class Merge:
	def __init__(self, redis, callback, queue):
		self.r = redis
		self.c = callback
		self.q = queue
		asyncio.ensure_future(self.c(self.q))

	def __aiter__(self):
		return self

	async def __anext__(self):
		res = await self.q.get()
		return res
