import uuid
import time
import json
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
        node_id: Optional[str] = None,
        w: Optional[int] = None
    ) -> None:
        
        self.payload = payload
        # self.id = id or self._generate_id()
        self.id = -1
        self.t0 = time.time()
        self.node_id = node_id
        self.w = w

    def _generate_id(self) -> str:
        return uuid.uuid4().hex[:16]

    def __repr__(self):
        return json.dumps(self.to_dict())

    def to_dict(self):
        _v = self.__dict__
        return {k: _v[k] for k in self.__serialize__}

    def from_dict(self, obj):
        item = Item()
        item.__dict__.update(obj)
        return item


class LogRequest(BaseModel):
    id: Optional[str] # = Field(..., title="Message ID")
    t0: Optional[float] # = Field(..., title="Message timestamp")
    node_id: Optional[str] # = Field(..., title="Node ID")
    w: Optional[int] = 1
    payload: Dict[str, Any] = Field({'msg': 'message'}, title="Object to log")


class LogListResponse(BaseModel):
    # logs: List[LogResponse] = []
    __root__: List[LogRequest]


def req2item(req: LogRequest) -> Item:
    item = Item().from_dict(req)
    return item

def item2resp(item: Item) -> LogRequest:
    resp = LogRequest(
        id=item.id,
        t0=item.t0,
        node_id=item.node_id,
        payload=item.payload
    )
    return resp


class RegisterSecondaryResponse(BaseModel):
    status: bool = Field(..., title="Status of secondary registering operation")

class RegisterSecondaryRequest(BaseModel):
    node_id: str = Field(..., title="Secondary node ID")
    url: str = Field(..., title="Secondary URL")
    role: str = Field(..., title="Secondary role")
