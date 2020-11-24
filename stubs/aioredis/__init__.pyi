from collections import OrderedDict
from typing import Any
from typing import Optional
from typing import List
from typing import Tuple
from typing import Dict
from typing import Union


ReadMessageType = Tuple[bytes, bytes, OrderedDict[bytes, bytes]]
RangeMessageType = Tuple[bytes, OrderedDict[bytes, bytes]]


class CommandsMixin:
	async def xread(
		self,
		streams: List[str],
		timeout: int = 0,
		count: Optional[int] = None,
		latest_ids: Optional[List[bytes]] = None
	) -> List[ReadMessageType]: ...

	async def xrange(
		self,
		stream: str,
		start: str = '+',
		stop: str = '-',
		count: Optional[int] = None
	) -> List[RangeMessageType]: ...

	async def xrevrange(
		self,
		stream: str,
		start: str = '+',
		stop: str = '-',
		count: Optional[int] = None
	) -> List[RangeMessageType]: ...

	async def xadd(
		self,
		stream: str,
		fields: Dict[str, Union[bytes, float, int, str]],
	) -> bytes: ...


class Redis(CommandsMixin):

	def __init__(self, pool_or_conn: Any) -> None: ...

	def close(self) -> None: ...

	async def wait_closed(self) -> None: ...

	async def flushall(self) -> None: ...


async def create_redis(address: str) -> Redis: ...
