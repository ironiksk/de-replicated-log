import time
import asyncio
from urllib.parse import urljoin
from typing import List, Optional
from const import Item, LogNodeType, req2item

from utils import async_post, async_get, async_put, post, get

import logging
logging.basicConfig(level=logging.INFO)
from threading import Thread

def urljoin(url, req):
    return url + req

class RLog(object):
    """docstring for RLog"""
    def __init__(self):
        self.__db = dict()

    def get_uuid(self):
        # mandatory condition ID1 < ID2
        return str(len(self.__db))

    def get_all(self):
        return list(self.__db.values())

    def get(self, _id):
        return self.__db.get(_id, None)

    def append(self, item):
        # QQ: implement linked-list to get previous message
        # WARNING: assigned outside
        # item.id = self.get_uuid()
        
        # TEST: add delay before appending
        # time.sleep(5)

        self.__db[item.id] = item
        return item


class RLogLocal(RLog):
    def __init__(self, node_id: str):
        super().__init__()
        self._node_id = node_id

    def get_all(self) -> List[Item]:
        return super().get_all()

    def append(self, item: Item) -> Item:
        item.node_id = self._node_id
        return super().append(item)


class RLogRemote(RLog):
    def __init__(self, url, role='master'):
        self._url = url
        self._role = role

    def healthcheck(self):
        try:
            return get(self._url + '/healthcheck')['status'] == 'success'
        except Exception as e:
            return False
        
    def get_all(self):
        resp = get(self._url + '/logs')
        items = [req2item(r) for r in resp]
        return items

    def get(self, _id):
        resp = get(self._url + '/log/' + _id)
        return req2item(resp)

    def append(self, item):
        resp = post(self._url + '/log/' + item.id, item.to_dict())
        return req2item(resp)
        


class RLogServer(object):
    """docstring for RLogServer"""
    def __init__(self, url:str, master_url:str = None):
        super(RLogServer, self).__init__()

        import uuid
        node_id = uuid.uuid4().hex[:16]

        self._node = RLogLocal(node_id=node_id) # internal rlog object
        self.master_url = master_url
        self._url = url

        # run healthcheck of all linked nodes
        self._do_healthcheck = True
        self._threads_healthcheck = []

        self._nodes = []
        if self.master_url:
            # register this node on master
            self.add_node_on_master()
    

    def stop(self):
        self._do_healthcheck = False
        [t.join() for t in self._threads_healthcheck];


    def _node_healthcheck_thread(self, node):
        retries = 0
        while self._do_healthcheck:
            # TODO: probably process healthchecks in parralel
            if not node.healthcheck():
                logging.warning(f'Target {node._url} not healthy... Waiting...')
                retries += 1
            else:
                retries = 0
                logging.info(f'Healthcheck OK {node._url}')
            if retries > 3:
                logging.error(f'Target {node._url} not healthy... Remove...')
                # WARNING: not thread-safe. TODO: add sync privitive
                self._nodes.remove(node)
                break
            time.sleep(2)


    def add_node(self, url, role):
        logging.info(f'Add `{role}` node with {url}')
        node = RLogRemote(url=url, role=role)
        self._threads_healthcheck.append(
            Thread(target=self._node_healthcheck_thread, args=(node,))
        )
        self._threads_healthcheck[-1].start()
        self._nodes.append(node)
        return True
        
    def add_node_on_master(self):
        # TODO: process exception if cannot register target
        ret = post(self.master_url + '/register', {
            'url': self._url,
            'role': 'secondary',
            'node_id': self._node._node_id
        })
        if ret['status'] == 'success':
            self.add_node(self.master_url, 'master')
            return True
        return False

    def get_uuid_item(self):
        return self._node.get_uuid()

    def get(self, log_id) -> Item:
        return self._node.get(log_id)

    def get_all(self) -> List[Item]:
        return self._node.get_all()

    def append(self, item: Item) -> Item:
        item.t0 = time.time()
        if not self.master_url:
            # generate unique id for item in master node, should be replicated to others
            item.id = self.get_uuid_item()
            self._append_on_secondaries(item)
        return self._node.append(item)
        
    def _append_on_secondaries(self, item):
        results = [None] * len(self._nodes)

        def _append_on_node(i, node, item, result):
            res = node.append(item)
            result[i] = res

        pool = [None] * len(self._nodes)
        for i, node in enumerate(self._nodes):
            pool[i] = Thread(target=_append_on_node, args=(i, node, item, results))
            pool[i].start()
        
        # TODO: smart joining to do not wait all threads 
        for i, node in enumerate(self._nodes):
            pool[i].join()

        # TODO: process exception if cannot append target on node
        for i, node in enumerate(self._nodes):
            # it = node.append(item)
            assert results[i].id == item.id, 'ID for item in Secondaary should match with local'
        return True


