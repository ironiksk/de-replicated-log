import json
import aiohttp

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