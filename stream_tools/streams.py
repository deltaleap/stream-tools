import asyncio

import aioredis

from .join import Join
from .merge import Merge


class Streams(Merge, Join):
	def __init__(self, stream_list):
		self.stream_list = list(stream_list)
		self.stream_names = [s.name for s in stream_list]
		self.q = asyncio.Queue()

	async def __aenter__(self):
		self.r = await aioredis.create_redis(
			'redis://localhost'
		)

		asyncio.ensure_future(self._reads())

		return self

	async def __aexit__(self, exception_type, exception, traceback):
		self.r.close()

	def __aiter__(self):
		return self
	
	async def __anext__(self):
		res = await self.q.get()
		return res

	async def _reads(self):
		while True:
			res = await self.r.xread(
				self.stream_names,
				count=1
			)
			await self.q.put(res[0])
