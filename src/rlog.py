import time
import asyncio
from urllib.parse import urljoin
from typing import List, Optional
from const import Item, LogNodeType, req2item

from utils import async_post, async_get, async_put, post, get

import logging
logging.basicConfig(level=logging.INFO)
from threading import Thread, Condition

def urljoin(url, req):
    return url + req


class CountDownLatch():
    def __init__(self, count):
        # store the count
        self.count = count
        self.condition = Condition()
 
    def count_down(self):
        with self.condition:
            logging.info(f'CountDownLatch {self.count}')
            if self.count == 0:
                return
            self.count -= 1
            if self.count == 0:
                self.condition.notify_all()
 
    # wait for the latch to open
    def wait(self):
        # acquire the lock on the condition
        with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return
            # wait to be notified when the latch is open
            self.condition.wait()


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
        # Simplest deduplication
        if item.id in self.__db:
            return item

        self.__db[item.id] = item
        return item

    def data_version(self):
        return len(self.__db) # sorted(self.__db.keys())[-1]


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
            logging.error(f'Error during requesting secondary: {e}')
            return False
        
    def get_all(self, r:int=1):
        try:
            resp = get(self._url + f'/logs?r={r}', timeout=None)
            items = [req2item(r) for r in resp]
            return items
        except Exception as e:
            logging.error(f'Error during requesting secondary: {e}')
            return []

    def get(self, _id):
        try:
            resp = get(self._url + '/log/' + _id, timeout=None)
            return req2item(resp)
        except Exception as e:
            logging.error(f'Error during requesting secondary: {e}')
            return None

    def append(self, item):
        try:
            resp = post(self._url + '/log/' + item.id, item.to_dict(), timeout=None)
            return req2item(resp)
        except Exception as e:
            logging.error(f'Error during requesting secondary: {e}')
            return None
        
    def data_version(self):
        try:
            resp = get(self._url + f'/version')
            return resp['version']
        except Exception as e:
            logging.error(f'Error during requesting secondary: {e}')
            return None



class RLogServer(object):
    """docstring for RLogServer"""
    def __init__(self, url:str, master_url:str = None):
        super(RLogServer, self).__init__()

        import uuid
        node_id = uuid.uuid4().hex[:16]

        self._node = RLogLocal(node_id=node_id) # internal rlog object
        self._master_node = None
        self.master_url = master_url
        self._url = url

        # run healthcheck of all linked nodes
        self._do_healthcheck = True
        self._do_data_sync = True

        self._threads_healthcheck = []

        self._nodes = []
        if self.master_url:
            # server started as secondary node, register this node on master
            if self.add_node_on_master():
                self._master_node = RLogRemote(url=self.master_url)
                self._sync_thread = Thread(target=self._sync_data_thread)
                self._sync_thread.start()
    

    def stop(self):
        self._do_healthcheck = False
        self._do_data_sync = False
        [t.join() for t in self._threads_healthcheck];
        if self._master_node:
            self._sync_thread.join()


    def _node_healthcheck_thread(self, node):
        retries = 0
        default_delay_s = 2
        max_delay_s = 30
        while self._do_healthcheck:
            # TODO: probably process healthchecks in parralel
            if not node.healthcheck():
                logging.warning(f'Target {node._url} not healthy... Waiting...')
                retries += 1
            else:
                retries = 0
                logging.info(f'Healthcheck OK {node._url}')
            if retries > 3:
                logging.error(f'Target {node._url} not healthy...')
                # # WARNING: not thread-safe. TODO: add sync privitive
                # self._nodes.remove(node)
                # break
            time.sleep(min(default_delay_s + retries, max_delay_s))


    def _sync_data_thread(self):
        # call from secondary to sync data with master
        default_delay_s = 2
        while self._do_data_sync:
            master_version = self._master_node.data_version()
            local_version = self._node.data_version()
            if master_version != local_version:
                logging.info(f"Initiate secondary syncronization. Master: {master_version} Secondary: {local_version}")
                # TODO: add linked list logic to reduce need of getting all items
                [self._node.append(item) for item in self._master_node.get_all()]
            time.sleep(default_delay_s)



    def add_node(self, url, role):
        # Call on master node to register secondary.
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
        # call method from secondary node to register self on master
        ret = post(self.master_url + '/register', {
            'url': self._url,
            'role': 'secondary',
            'node_id': self._node._node_id
        })
        if ret['status'] == 'success':
            self.add_node(self.master_url, 'master')
            return True
        return False

    def data_version(self):
        return self._node.data_version()

    def get_uuid_item(self):
        return self._node.get_uuid()

    def get(self, log_id) -> Item:
        return self._node.get(log_id)

    def get_all(self, r=1) -> List[Item]:

        def vote(items):
            # vouter by id 
            id2count = {}
            max_i = 0
            max_id = items[max_i].id
            for i, it in enumerate(items):
                id2count[it.id] = id2count.get(it.id, 0) + 1
                if id2count[it.id] > id2count[max_id]:
                    max_id = it.id
                    max_i = i
            return items[max_i]

        items = {it.id: [it] for it in self._node.get_all()}
        if r > 1:
            results = [None] * len(self._nodes)
            clatch = CountDownLatch(count=r-1) # r-1 - do not count local (master buffer)
            def _read_on_node(i, node, result, latch=None):
                res = node.get_all()
                result[i] = res
                latch.count_down()
                logging.info(f'Request for {i} finished with result {res}')

            pool = [None] * len(self._nodes)
            for i, node in enumerate(self._nodes):
                pool[i] = Thread(target=_read_on_node, args=(i, node, results, clatch))
                pool[i].start()
            
            # TODO: smart joining to do not wait all threads 
            # for i, node in enumerate(self._nodes):
            #     pool[i].join()
            clatch.wait()

            for res in results:
                if not res:
                    continue
                for i in res:
                    items[i.id].append(i)

        assert all([len(items[id])>=r for id in items]), 'Cannot get item, consensus has not achieveds'
        # voting for items in map
        items = [vote(items[id]) for id in items]
        return items

    def append(self, item: Item) -> Item:
        item.t0 = time.time()
        if not self.master_url:
            # generate unique id for item in master node, define total ordering, should be replicated to others
            item.id = self.get_uuid_item()
            self._append_on_secondaries(item)
        return self._node.append(item)
        
    def _append_on_secondaries(self, item):
        results = [None] * len(self._nodes)

        clatch = CountDownLatch(count=item.w-1) # w-1 - do not count local (master buffer)

        def _append_on_node(i, node, item, result, latch=None):
            res = node.append(item)
            result[i] = res
            latch.count_down()
            logging.info(f'Request for {i} finished with result {res}')

        pool = [None] * len(self._nodes)
        for i, node in enumerate(self._nodes):
            pool[i] = Thread(target=_append_on_node, args=(i, node, item, results, clatch))
            pool[i].start()
        
        # TODO: smart joining to do not wait all threads 
        # for i, node in enumerate(self._nodes):
        #     pool[i].join()
        clatch.wait()

        # TODO: process exception if cannot append target on node
        retc = 0
        for i, node in enumerate(self._nodes):
            # it = node.append(item)
            if not results[i]:
                logging.warning(f'Request from thread {i} has not yet received. Skip...')
                continue
            assert results[i].id == item.id, 'ID for item in Secondaary should match with local'
            retc+=1
        assert retc < item.w, 'Cannot append item, consensus has not achieveds'

        return True


# If message delivery fails (due to connection, or internal server error, or secondary is unavailable) the delivery attempts should be repeated - retry
#   If one of the secondaries is down and w=3, the client should be blocked until the node becomes available. The client that is running in parallel shouldn’t be blocked by the blocked one.
#   If w>1 the client should be blocked until the message will be delivered to all secondaries required by the write concern level. The client that is running in parallel shouldn’t be blocked by the blocked one.
#   All messages that secondaries have missed due to unavailability should be replicated after (re)joining the master
#   Retries can be implemented with an unlimited number of attempts but, possibly, with some “smart” delays logic
#   You can specify a timeout for the master in the case if there is no response from the secondary
# All messages should be present exactly once in the secondary log - deduplication
#   To test deduplication you can generate some random internal server error response from the secondary after the message has been added to the log
# The order of messages should be the same in all nodes - total order
#   If secondary has received messages [msg1, msg2, msg4], it shouldn’t display the message ‘msg4’ until the ‘msg3’ will be received
#   To test the total order, you can generate some random internal server error response from the secondaries

