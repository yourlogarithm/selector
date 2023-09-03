import asyncio
import datetime
import traceback

import aiohttp
from redis import asyncio as aioredis
from common_utils.logger import Logger

from settings import Settings

settings = Settings()

GROUP_NAME = 'selector'
CRAWLER_ENDPOINT = settings.crawler_uri + '/crawl?url={}'

DOMAIN_HEAP_QUEUE = 'domain_heap_queue'


async def cleanup(redis: aioredis.Redis, stream_name: str) -> bool:
    if await redis.xlen(stream_name) == 0:
        await asyncio.gather(*(redis.delete(stream_name), redis.zrem(DOMAIN_HEAP_QUEUE, stream_name)))
        return True
    return False


async def process(client: aiohttp.ClientSession, redis: aioredis.Redis, semaphore: asyncio.Semaphore, stream_name: bytes, timestamp: float, logger: Logger):
    try:
        async with semaphore:
            if (diff := timestamp - datetime.datetime.now().timestamp()) > 0:
                logger.debug(f'Sleeping {diff} for {stream_name}')
                await asyncio.sleep(diff)
            stream_name = stream_name.decode()
            _, messages = (await redis.xread({stream_name: 0}, count=1))[0]
            for message_id, record in messages:
                url = record[b'url'].decode()
                start_time = datetime.datetime.now().timestamp()
                logger.info(f'Sending {url} to crawler')
                await client.post(CRAWLER_ENDPOINT.format(url))
                now = datetime.datetime.now().timestamp()
                elapsed = now - start_time
                if not await cleanup(redis, stream_name):
                    next_crawl_time = elapsed * 10 + now
                    await redis.zadd(DOMAIN_HEAP_QUEUE, {stream_name: next_crawl_time})
                    logger.debug(f'Updating next crawl time for {stream_name} - {next_crawl_time}')
    except Exception:
        traceback.print_exc()


async def main():
    client = aiohttp.ClientSession()
    redis_client = await aioredis.from_url(settings.redis_uri)
    task_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(settings.max_workers)
    logger = Logger('selector', settings.log_level)
    logger.info('Starting...')
    try:
        while True:
            records = await redis_client.zpopmin(DOMAIN_HEAP_QUEUE, 1)
            if records:
                tasks = [task_queue.put(asyncio.create_task(process(client, redis_client, semaphore, stream_name, timestamp, logger))) for stream_name, timestamp in records]
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(1)
    finally:
        await task_queue.join()
        await client.close()


if __name__ == '__main__':
    asyncio.run(main())
