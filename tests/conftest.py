import aioredis
import pytest


@pytest.fixture
async def redis():
    """Return redis client instance
    """
    redis = await aioredis.create_redis('redis://localhost')
    yield redis
    await redis.flushall()
    redis.close()
