import asyncio
from urllib.parse import urljoin

from utils import async_post, async_get

import logging
logging.basicConfig(level=logging.DEBUG)


class RLog(object):
    """docstring for RLog"""
    def __init__(self):
        super(RLog, self).__init__()
        self.__db = dict()

    def get_uuid(self):
        return len(self.__db)

    def get_all(self):
        return list(self.__db.items())

    def get(self, _id: int):
        return self.__db.get(_id, None)

    def append(self, msg: str):
        _id = self.get_uuid()
        self.__db[_id] = msg
        return _id, msg


class RLogMaster(RLog):
    """docstring for RLogMaster"""
    def __init__(self, role:str, master:str=None, replicas: list= []):
        super(RLogMaster, self).__init__()
        self.replicas = replicas
        self.master = master
        self.role = role

    async def replicate_data(self, msg):
        results = await asyncio.gather(*[
            async_post(urljoin(rurl, '/log'), {'msg': msg}) for rurl in self.replicas
        ])
        return results

    async def append(self, msg: str):
        _id, _msg = super().append(msg)

        if self.role == 'MASTER':
            results = await self.replicate_data(msg)
            for rurl, result in zip(self.replicas, results):
                logging.debug(f'Received results from {rurl}: {result}')
                assert _id == result['id'], f'Local and remote IDs differ {_id} =! {result["id"]}'

        return _id, _msg

         


