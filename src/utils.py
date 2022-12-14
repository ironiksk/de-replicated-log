import json
import aiohttp
import aiohttp_retry
import requests
from requests.adapters import HTTPAdapter, Retry


async def async_post_retry(url, data, retry_attempts=10):
    statuses = {x for x in range(100, 600)}
    statuses.remove(200)
    statuses.remove(429)    
    result = {}
    async with aiohttp.ClientSession() as session:
        retry_session = aiohttp_retry.RetryClient(session)
        async with retry_session.post(url, json=data, retry_attempts=10, retry_for_statuses=statuses) as resp:
            result = await resp.json()
        await retry_client.close()
        return result

async def async_post(url, data):
    async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
        async with session.post(url, json=data) as resp:
            result = await resp.json()
            return result

async def async_get(url):
    async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
        async with session.get(url) as resp:
            result = await resp.json()
            return result

async def async_put(url, data):
    async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
        async with session.put(url, json=data) as resp:
            result = await resp.json()
            return result


def post(url, data, timeout=10):
    ret = requests.post(url, json = data, timeout=timeout)
    return ret.json()


def get(url, timeout=10):
    ret = requests.get(url, timeout=timeout)
    return ret.json()


def post_retry(url, data, timeout=10, retry_attempts=10):
    s = requests.Session()
    retries = Retry(total=timeout,
                    backoff_factor=0.1,
                    # status_forcelist=[ 500, 502, 503, 504 ]
                    )
    ret = s.mount(url, HTTPAdapter(max_retries=retries))
    # s.get('http://httpstat.us/500')

