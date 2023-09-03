import asyncio
import datetime
import os
import traceback

import aiohttp
from redis import asyncio as aioredis

GROUP_NAME = 'selector'
CONSUMER_NAME = os.getenv('CONSUMER_NAME', 'selector0')
MAX_WORKERS = os.getenv('MAX_WORKERS', 10)

CRAWLER_URL = os.getenv('CRAWLER_URL', 'http://localhost:8000')
CRAWLER_ENDPOINT = CRAWLER_URL + '/crawl?url={}'

DOMAIN_HEAP_QUEUE = 'domain_heap_queue'


async def cleanup(redis: aioredis.Redis, stream_name: str) -> bool:
    if await redis.xlen(stream_name) == 0:
        await asyncio.gather(*(redis.delete(stream_name), redis.zrem(DOMAIN_HEAP_QUEUE, stream_name)))
        return True
    return False


async def process(client: aiohttp.ClientSession, redis: aioredis.Redis, semaphore: asyncio.Semaphore, stream_name: bytes, timestamp: float):
    try:
        async with semaphore:
            if (diff := timestamp - datetime.datetime.now().timestamp()) > 0:
                print(f'Sleeping {diff} for {stream_name}')
                await asyncio.sleep(diff)
            stream_name = stream_name.decode()
            _, messages = (await redis.xread({stream_name: 0}, count=1))[0]
            for message_id, record in messages:
                url = record[b'url'].decode()
                start_time = datetime.datetime.now().timestamp()
                print(f'Sending {url} to crawler')
                await client.post(CRAWLER_ENDPOINT.format(url))
                now = datetime.datetime.now().timestamp()
                elapsed = now - start_time
                if not await cleanup(redis, stream_name):
                    next_crawl_time = elapsed * 10 + now
                    await redis.zadd(DOMAIN_HEAP_QUEUE, {stream_name: next_crawl_time})
                    print(f'Updating next crawl time for {stream_name} - {next_crawl_time}')
    except Exception:
        traceback.print_exc()


async def main():
    client = aiohttp.ClientSession()
    redis_client = await aioredis.from_url(os.getenv("REDIS_URI"))
    task_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    print('Starting...')
    try:
        while True:
            records = await redis_client.zpopmin(DOMAIN_HEAP_QUEUE, 1)
            if records:
                tasks = [task_queue.put(asyncio.create_task(process(client, redis_client, semaphore, stream_name, timestamp))) for stream_name, timestamp in records]
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(1)
    finally:
        await task_queue.join()
        await client.close()


if __name__ == '__main__':
    asyncio.run(main())
