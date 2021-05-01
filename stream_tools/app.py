import asyncio
import functools

import aioredis
from loguru import logger


class App:
    def __init__(
        self,
        url='redis://localhost',
        encoding='utf-8',
        decode_responses=False,
        loop=None
    ):
        self.streams = []
        self.agent_map = {}
        self.merge_map = {}
        self.stream_last_ids = {}

        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

        self.decode_responses = decode_responses
        self.r = aioredis.from_url(
            url,
            encoding=encoding,
            decode_responses=decode_responses
        )

    def agent(self, stream):
        def actual_decorator(func):
            self._map_agent(stream, func)
            @functools.wraps(func)  # delete
            def func_wrapper(*args, **kwargs):  # delete
                # logger.debug(f"decorating {func.__name__}")
                # logger.debug(f"now all streams are: {self.streams}")
                # logger.debug(f"added {stream} stream")
                # logger.debug(f"app: {self.agent_map}")
                return func(*args, **kwargs)
            return func_wrapper
        return actual_decorator

    def merge(self, streams):
        def decorator(func):
            for stream in streams:
                self._map_merge(stream, func)
            return func
        return decorator

    def _map_agent(self, stream, func) -> None:
        if not self.decode_responses:
            stream = stream.encode()
        if not stream in self.agent_map:
            self.agent_map[stream] = []
            self.streams.append(stream)
            self.stream_last_ids[stream] = 0
        self.agent_map[stream].append((func, func.__name__))

    def _map_merge(self, stream, func) -> None:
        if not self.decode_responses:
            stream = stream.encode()
        if not stream in self.merge_map:
            self.merge_map[stream] = []
            self.streams.append(stream)
            self.stream_last_ids[stream] = 0
        self.merge_map[stream].append((func, func.__name__))

    def _update_last_ids(self, stream, last_id):
        self.stream_last_ids[stream] = last_id

    async def _get(self):
        while True:
            res = await self.r.xread(self.stream_last_ids, block=1000)
            """
            res:
            [
                [b'stream_1', [
                    (b'1619544330785-0', {b'val': b'1.117'}),
                    (b'1619544331587-0', {b'val': b'1.464'}),
                    (b'1619544332389-0', {b'val': b'1.274'}),
                    (b'1619544333191-0', {b'val': b'0.856'}),
                    (b'1619544333993-0', {b'val': b'1.490'})
                    ]
                ]
            ]
            """
            for s in res:
                self._update_last_ids(
                    s[0],  # stream_name
                    s[1][-1][0]  # id of the last elements
                )

            for s in res:
                try:
                    for f, f_name in self.agent_map[s[0]]:
                        print(1)
                        logger.debug(f"executing agent: {f_name}({s[0]})")
                        for row in s[1]:
                            await f(row)
                except KeyError as e:
                    pass
                try:
                    for f, f_name in self.merge_map[s[0]]:
                        logger.debug(f"executing merge: {f_name}({s[0]})")
                        for row in s[1]:
                            await f(s[0], row)
                except IndexError as e:
                    pass

    async def _warn_agent(self):
        res = await self.r.scan()
        streams_in_redis = res[1]
        not_in_redis = [
            i
            for i in self.streams
            if not i in streams_in_redis
        ]
        not_in_agents = [
            i
            for i in streams_in_redis
            if not i in self.streams
        ]
        if not_in_redis:
            logger.warning(f"Streams not in redis: {not_in_redis}")
        if not_in_agents:
            logger.warning(f"Streams without agents: {not_in_agents}")

    async def _run(self):
        if self.debug:
            asyncio.create_task(self._warn_agent())

        self.task = asyncio.create_task(self._get())

        while True:
            await asyncio.sleep(60)

    def run(self, debug=False):
        self.debug = debug
        if self.debug:
            logger.level("DEBUG")
            logger.debug(self.loop)
        else:
            logger.remove()
        self.loop.set_debug(debug)
        self.loop.run_until_complete(self._run())
