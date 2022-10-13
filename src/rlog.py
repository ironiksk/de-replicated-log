import asyncio
from urllib.parse import urljoin
from typing import List, Optional
from const import Item, LogNodeType

from utils import async_post, async_get, async_put

import logging
logging.basicConfig(level=logging.DEBUG)


class RLog(object):
    """docstring for RLog"""
    def __init__(self):
        self.__db = dict()

    def get_uuid(self):
        return len(self.__db)

    def get_all(self):
        return list(self.__db.values())

    def get(self, _id):
        return self.__db.get(_id, None)

    def append(self, item):
        # _id = self.get_uuid()
        self.__db[item.id] = item
        return item


def rlog_builder(role: LogNodeType, **kwargs):
    import uuid
    node_id = uuid.uuid4().hex[:16]
    if role == LogNodeType.MASTER:
        return RLogMaster(node_id=node_id)
    else:
        return RLogSecondary(node_id=node_id, master=kwargs['master'], port=kwargs['port'])


class RLogMaster(RLog):
    """docstring for RLogMaster"""
    def __init__(self, node_id: str, secondaries: List[str] = []):
        super().__init__()
        self._node_id = node_id
        self._secondaries = secondaries

    async def add_secondary(self, secondary_id, url):
        if not await self._check_secondary_health(secondary_id, url):
            return False
        self._secondaries.append(url)
        # TODO: initiate data transfering
        return True

    async def _check_secondary_health(self, secondary_id, url):
        await asyncio.sleep(1)
        try:
            await asyncio.wait_for(async_get(urljoin(url, '/healthcheck')), timeout=1.0)
        except asyncio.TimeoutError:
            return False
        return True
            

    async def _append_on_secondaries(self, item):
        # send data to secondary
        results = await asyncio.gather(*[
            async_post(urljoin(rurl, f'/log/{item.id}'), item.payload) for rurl in self._secondaries
        ])
        status = True
        for rurl, result in zip(self._secondaries, results):
            logging.debug(f'Received results from {rurl}: {result}')
            status = status and item.id == result['id']
        return status

    async def get_all(self):
        return super().get_all()

    async def append(self, item: Item) -> Item:
        ret = await self._append_on_secondaries(item)
        item.node_id = self._node_id
        return super().append(item)



class RLogSecondary(RLog):
    """docstring for RLogMaster"""
    def __init__(self, node_id:str, master: str, port:int):
        super().__init__()
        self._node_id = node_id
        self._master_url = master
        self._port = port

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._register_on_master())

    async def _register_on_master(self):
        payload = {
            'node_id': self._node_id,
            'port': self._port
        }
        status = await async_post(urljoin(self._master_url, '/register'), payload)
        return status

    async def get_all(self):
        return super().get_all()

    async def append(self, item: Item) -> Item:
        return super().append(item)


# class RLog:
#     """docstring for RLogMaster"""
#     def __init__(self, role: LogNodeType, master: Optional[str]=None):
#         super(RLog, self).__init__()
#         self.__master = master
#         self.__role = role
#         self.__db = dict()

#         if self.__role == LogNodeType.SECONDARY:
#             # register
#             # start syncing sync
#             # start syncing thread

#         # else: # self.__role == LogNodeType.MASTER:
#         # self.__last_item = None


#     async def append(self, item: Item) -> Item:
#         self.__db[item.id] = item
#         return item
        

#     async def get_all(self) -> List[Item]:
#         return list(self.__db.values())


#     async def _register_secondary(self):
#         # register secondary node on master
#         pass

#     async def _syncronize_with_master(self):
#         # receive history with master node
#         pass


    # async def replicate_data(self, msg):
    #     results = await asyncio.gather(*[
    #         async_post(urljoin(rurl, '/log'), {'msg': msg}) for rurl in self.replicas
    #     ])
    #     for rurl, result in zip(self.replicas, results):
    #         logging.debug(f'Received results from {rurl}: {result}')
    #         assert _id == result['id'], f'Local and remote IDs differ {_id} =! {result["id"]}'

    #     return results

    # async def append(self, msg: str):
    #     _id, _msg = super().append(msg)

    #     if self.role == 'MASTER':
    #         results = await self.replicate_data(msg)

    #     return _id, _msg

         


