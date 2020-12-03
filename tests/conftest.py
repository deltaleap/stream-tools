from typing import AsyncGenerator

import aioredis
import pytest  # type: ignore


@pytest.fixture
async def redis() -> AsyncGenerator:
    """Return redis client instance"""
    redis = await aioredis.create_redis("redis://localhost")
    yield redis
    await redis.flushall()
    redis.close()
    await redis.wait_closed()
