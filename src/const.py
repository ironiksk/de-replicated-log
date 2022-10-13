import uuid
import time
from enum import Enum
from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field


class LogNodeType(Enum):
    MASTER = 'master'
    SECONDARY = 'secondary'
    def __str__(self):
        return self.value


class Item:
    __serialize__ = [
        'id',
        'payload',
        'node_id',
        't0'
    ]
    def __init__(self, 
        payload: Optional[Dict[str, Any]] = None, 
        id: Optional[str] = None, 
        node_id: Optional[str] = None
    ) -> None:
        
        self.payload = payload
        self.id = id or self._generate_id()
        self.t0 = time.time()
        self.node_id = node_id

    def _generate_id(self) -> str:
        return uuid.uuid4().hex[:16]

    def to_dict(self):
        _v = self.__dict__
        return {k: _v[k] for k in self.__serialize__}

    def from_dict(self, obj):
        item = Item()
        item.__dict__.update(obj)
        return item


class LogRequest(BaseModel):
    msg: str = Field(..., title="Message")

class LogResponse(LogRequest):
    id: str = Field(..., title="Message ID")

class LogListResponse(BaseModel):
    logs: List[LogResponse] = []


def req2item(req: LogRequest) -> Item:
    item = Item(
        payload={
            'msg': req.msg
        }
    )
    return item

def item2resp(item: Item) -> LogResponse:
    resp = LogResponse(
        id=item.id,
        msg=item.payload['msg']
    )
    return resp


class RegisterSecondaryResponse(BaseModel):
    status: bool = Field(..., title="Status of secondary registering operation")

class RegisterSecondaryRequest(BaseModel):
    node_id: str = Field(..., title="Secondary node ID")
    port: int = Field(..., title='Secondary API port')
