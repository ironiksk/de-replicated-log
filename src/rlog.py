import time
import asyncio
from functools import wraps
from urllib.parse import urljoin
from typing import List, Optional
from enum import Enum
from const import Item, LogNodeType, req2item

from utils import async_post, async_get, async_put, post, get
from worker import BaseWorker

import logging
logging.basicConfig(level=logging.INFO)
from threading import Thread, Condition

def urljoin(url, req):
    return url + req


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

    def healthy(self):
        return False

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
        # if item.id in self.__db:
        #     return item
        assert item.id not in self.__db, 'Object with specified ID already exists'

        self.__db[item.id] = item
        return item

    @property
    def data_version(self):
        return len(self.__db) # sorted(self.__db.keys())[-1]


class RLogLocal(RLog):
    def __init__(self, node_id: str, url:str, role: str):
        super().__init__()
        self._node_id = node_id
        self._role = role
        self._url = url

    @property
    def id(self):
        return self._node_id

    @property
    def role(self):
        return self._role

    @property
    def url(self):
        return self._url

    def healthy(self):
        return True

    def get_all(self) -> List[Item]:
        return super().get_all()

    def append(self, item: Item) -> Item:
        item.node_id = self._node_id
        return super().append(item)


class RLogRemote(RLog):
    def __init__(self, node_id, url, role='master'):
        self._url = url
        self._role = role
        self._node_id = node_id

    @staticmethod
    def info(url):
        try:
            resp = get(url + f'/info')
            return resp
        except Exception as e:
            logging.error(f'Error during requesting secondary: {e}')
            return None

    @staticmethod
    def from_url(url):
        info = RLogRemote.info(url)
        return RLogRemote(node_id=info['node_id'], url=url, role=info['role'])

    @property
    def id(self):
        return self._node_id

    @property
    def role(self):
        return self._role

    @property
    def url(self):
        return self._url


    def healthy(self):
        try:
            return get(self._url + '/healthcheck', timeout=0.1)['status'] == 'success'
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
    
    @property
    def data_version(self):
        try:
            resp = get(self._url + f'/info')
            return resp['version']
        except Exception as e:
            logging.error(f'Error during requesting secondary: {e}')
            return None


class HealthChecker(BaseWorker):            
    def __init__(self):
        super(HealthChecker, self).__init__()
        self.__nodes = {} # id -> node instance
        self.__threads = {}
        self.__health = {}
        self.__dead = {}

    def healthy(self, node):
        return self.__health[node.id]

    def dead(self, node):
        return self.__dead.get(node.id, False)

    def _node_healthcheck_thread(self, node):
        retries = 0
        default_delay_s = 2
        max_delay_s = 30
        while self.should_keep_running():
            # TODO: probably process healthchecks in parralel
            self.__health[node.id] = False
            if not node.healthy():
                logging.warning(f'Target {node.url} not healthy... Waiting...')
                retries += 1
            else:
                retries = 0
                logging.info(f'Healthcheck OK {node.url}')
                self.__health[node.id] = True
            if retries > 3:
                logging.error(f'Target {node.url} not healthy...')
                self.__dead[node.id] = True
                # # WARNING: not thread-safe. TODO: add sync privitive
                # self._nodes.remove(node)
                # break
            time.sleep(min(default_delay_s + retries, max_delay_s))

    def add_node(self, node):
        if node.healthy():
            self.__nodes[node.id] = node
            self.__threads[node.id] = Thread(target=self._node_healthcheck_thread, args=(node,))
            self.__threads[node.id].start()
            self.__health[node.id] = True
            return True
        return False

    def del_node(self, node):
        if node.id not in self.__threads:
            return
        self.__threads[node.id].stop()
        del self.__threads[node.id]
        del self.__health[node.id]
        del self.__dead[node.id]

    def run(self):
        while self.should_keep_running():
            time.sleep(1)



class SecondaryStateManagement(BaseWorker):

    def __init__(self, local_node=None):
        super(SecondaryStateManagement, self).__init__()
        self.__local_node = local_node
        self.__nodes = {} # id -> node instance
        self.__threads = {}

    def _node_thread(self, node):
        retries = 0
        default_delay_s = 2
        max_delay_s = 30
        max_retries = 5
        while self.should_keep_running():
            # master is source of truth, master always knows the general order. To make synced secondary is it's responsibility
            if self.__local_node.role == 'master':
                if self.__local_node.data_version > node.data_version:
                    master_items = self.__local_node.get_all()
                    second_items = node.get_all()
                    for i in range(len(master_items)):
                        # TODO: make more clear expression
                        # TODO: make smart logic of list updating without rereading all messages. Could be problem becausse of performance
                        if len(second_items) < i and master_items[i].id != second_items[i].id:
                            node.append(master_items[i])
                            second_items = node.get_all()
                        elif len(second_items) >= i:
                            node.append(master_items[i])
                            second_items = node.get_all()
                else:
                    # If master is outdated...
                    pass
            
            time.sleep(min(default_delay_s + retries, max_delay_s))

    def add_node(self, node):
        if node.healthy():
            self.__nodes[node.id] = node
            self.__threads[node.id] = Thread(target=self._node_thread, args=(node,))
            self.__threads[node.id].start()
            return True
        return False

    def del_node(self, node):
        pass

    def run(self):
        while self.should_keep_running():
            time.sleep(1)




class RLogServer(object):
    def __init__(self, url:str, role:str):
        super(RLogServer, self).__init__()

        import uuid
        node_id = uuid.uuid4().hex[:16]
        self._nodes = [RLogLocal(node_id=node_id, url=url, role=role)]

        self._master_node = None
        self._local_node = self._nodes[0] # reference on self node
        
        self._hc_worker = HealthChecker()
        self._hc_worker.start()
        self._sc_worker = SecondaryStateManagement(self._local_node)
        self._sc_worker.start()


    @property
    def node(self):
        return self._local_node

    def stop(self):
        self._hc_worker.stop()

    def add_remote_node(self, url):
        # Function calls on node to register node from URL.
        # adds node to local buffer
        # - call from secondary
        # -- if added node is master - register on master
        # - call from master

        node = RLogRemote.from_url(url)

        # do not add already added node
        if any(map(lambda x: x.id == node.id, self._nodes )):
            return False

        logging.info(f'Add [{node.id}] `{node.role}` node with {node.url}')
        if not node.healthy():
            logging.warn(f'Node [{node.id}] with {url} unhealthy')
            return False

        self._nodes.append(node)
        self._hc_worker.add_node(node)
        self._sc_worker.add_node(node)

        if node.role == 'master':
            # TODO: make separate worker that handles handshake between master and secondary.
            ret = post(url + '/register', {
                'url': self._local_node.url,
                'role': self._local_node.role,
                'node_id': self._local_node.id
            })
            if ret['status'] == 'success':
                self._master_node = node

        return True

    def data_version(self):
        return self._local_node.data_version()

    def get_uuid_item(self):
        return self._local_node.get_uuid()

    def get(self, log_id) -> Item:
        return self._local_node.get(log_id)

    def _run_command_on_nodes(self, cmd, ccount, **kwargs):
        cnodes = len(self._nodes)-1
        assert ccount <= cnodes, f"Number of nodes {cnodes}+master is less than requested for consensus {ccount}+1"

        def _run_on_node(i, node, result, latch=None):
            # if node.healthy(): # TODO: could be a problem if node has uncestant state
            handler = getattr(node, cmd)
            result[i] = handler(**kwargs)
            latch.count_down()
            logging.info(f'Request for {i} finished with result {result[i]}')

        # results for all nodes
        results = [None] * (len(self._nodes)-1)
        clatch = CountDownLatch(count=ccount)

        # thread pool for one node
        pool = [None] * (len(self._nodes)-1)
        for i, node in enumerate(self._nodes[1:]):
            # TODO: move to finit pool because potential zombie thread cause here
            pool[i] = Thread(target=_run_on_node, args=(i, node, results, clatch))
            pool[i].start()
            
        # TODO: smart joining to do not wait all threads 
        # for i, node in enumerate(self._nodes):
        #     pool[i].join()
        clatch.wait()
        return results


    def get_all(self, r=1) -> List[Item]:
        if self._local_node.role == 'secondary':
            return self._local_node.get_all()

        items = {it.id: [it] for it in self._local_node.get_all()}

        if r > 1:
            results = self._run_command_on_nodes('get_all', ccount=r-1)

            # aggregate all items into one map ID->list[items]
            for res in results:
                if not res:
                    # skip because no response from node
                    continue
                for it in res:
                    if it.id not in items:
                        items[it.id] = [it]
                    else:
                        items[it.id].append(it)
        # TODO: what to do if no consensus for at least one item...
        assert all(map(lambda its: len(its)>=r, items.values())), 'No consensus found for one element. Break'
        
        # get 0 element from each list of results and return as array
        return [ items[i][0] for i in sorted(items.keys(), key=lambda x: int(x)) ]


    def append(self, item: Item) -> Item:
        item.t0 = time.time()
        if self._local_node.role == 'secondary':
            # WARNING: item.id should be specified in request, error occures otherwise
            return self._local_node.append(item)

        # generate unique id for item in master node, define total ordering, should be replicated to others
        item.id = self.get_uuid_item()

        # run command on secondaries
        results = self._run_command_on_nodes('append', ccount=item.w-1, item=item)

        ### process results from secondaries ###

        retc = 0
        for i, node in enumerate(self._nodes[1:]):
            if not results[i]:
                logging.warning(f'Request from thread {i} has not yet received. Skip...')
                continue
            assert results[i].id == item.id, 'ID for item in Secondaary should match with local'
            retc+=1
        assert retc >= item.w-1, 'Cannot append item, consensus has not achieved'

        return self._local_node.append(item)




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

