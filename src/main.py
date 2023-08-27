import asyncio
import os
import time
import aiohttp
from redis.asyncio import Redis

GROUP_NAME = 'selector'
CONSUMER_NAME = os.getenv('CONSUMER_NAME', 'selector0')
MAX_WORKERS = os.getenv('MAX_WORKERS', 10)

CRAWLER_URL = os.getenv('CRAWLER_URL', 'http://localhost:8000')
CRAWLER_ENDPOINT = CRAWLER_URL + '/crawl?url={}'

DOMAIN_HEAP_URL = os.getenv('DOMAIN_HEAP_URL', 'http://localhost:8001')
DOMAIN_ACQUIRE_ENDPOINT = DOMAIN_HEAP_URL + '/acquire'
DOMAIN_RELEASE_ENDPOINT = DOMAIN_HEAP_URL + '/release'
DOMAIN_RELEASE_HEADERS = {'Content-Type': 'application/json'}


async def process(client: aiohttp.ClientSession, redis: Redis, semaphore: asyncio.Semaphore, domain: str):
    async with semaphore:
        messages = await redis.xreadgroup(GROUP_NAME, CONSUMER_NAME, {domain: '>'}, count=1)
        if len(messages) == 0:
            print('WARNING: No messages found for domain {}'.format(domain))
        url = messages[0]['url']
        start_time = time.time()
        await client.post(CRAWLER_ENDPOINT.format(url))
        now = time.time()
        elapsed = now - start_time
        next_crawl_time = elapsed * 10 + now
        await client.post(DOMAIN_RELEASE_ENDPOINT, data={'domain': domain, 'timestamp': next_crawl_time}, headers=DOMAIN_RELEASE_HEADERS)


async def main():
    client = aiohttp.ClientSession()
    redis = Redis(host=os.getenv("REDIS_HOST", "localhost"), port=os.getenv("REDIS_PORT", 6379))
    task_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    try:
        while True:
            async with client.get(DOMAIN_HEAP_URL) as response:
                result = await response.json()
            if (domain := result['domain']) is None:
                await asyncio.sleep(1)
                continue
            await task_queue.put(asyncio.create_task(process(client, redis, semaphore, domain)))
        await task_queue.join()
    finally:
        await client.close()


if __name__ == '__main__':
    asyncio.run(main())
