import datetime
import unittest
from unittest.mock import AsyncMock, patch

from src.main import cleanup, process


class TestCrawlerModule(unittest.IsolatedAsyncioTestCase):
    @patch('redis.asyncio.Redis')
    async def test_cleanup(self, MockRedis):
        mock_redis = MockRedis()
        mock_redis.xlen = AsyncMock(return_value=0)
        mock_redis.delete = AsyncMock()
        mock_redis.zrem = AsyncMock()

        result = await cleanup(mock_redis, "test_stream")
        self.assertTrue(result)
        mock_redis.delete.assert_called_with("test_stream")
        mock_redis.zrem.assert_called_with("domain_heap_queue", "test_stream")

    @patch('src.main.aiohttp.ClientSession')
    @patch('redis.asyncio.Redis')
    @patch('src.main.asyncio.Semaphore')
    async def test_process(self, MockSemaphore, MockRedis, MockClientSession):
        mock_semaphore = MockSemaphore()
        mock_client = MockClientSession()
        mock_redis = MockRedis()
        mock_redis.xread = AsyncMock(return_value=[(b"test_stream", [(b"test_id", {b"url": b"http://example.com"})])])
        mock_semaphore.__aenter__ = AsyncMock()
        mock_semaphore.__aexit__ = AsyncMock()
        mock_client.post = AsyncMock()

        stream_name = b"test_stream"
        timestamp = datetime.datetime.now().timestamp() - 100

        with patch('src.main.cleanup', new_callable=AsyncMock) as mock_cleanup:
            mock_cleanup.return_value = False
            await process(mock_client, mock_redis, mock_semaphore, stream_name, timestamp)

        mock_client.post.assert_called()
        mock_redis.xread.assert_called()
        mock_redis.zadd.assert_called()
