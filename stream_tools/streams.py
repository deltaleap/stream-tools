import asyncio

import aioredis

# from .join import Join
from .merge import Merge


class Streams:
	def __init__(self, stream_list):
		self.stream_list = list(stream_list)
		self.stream_names = [s.name for s in stream_list]
		self.q = asyncio.Queue()

	async def __aenter__(self):
		self.r = await aioredis.create_redis(
			'redis://localhost'
		)

		asyncio.ensure_future(self._reads(self.q))

		return self

	async def __aexit__(self, exception_type, exception, traceback):
		self.r.close()


	def merge(self):
		merger = Merge(self.r, self._reads, self.q)
		return merger

	async def _reads(self, queue):
		while True:
			res = await self.r.xread(
				self.stream_names,
				count=1
			)
			await queue.put(res[0])
