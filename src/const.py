from typing import List, Optional, Literal
from pydantic import BaseModel, Field

class Message(BaseModel):
    msg: str = Field(..., title="Message")

class MessageResponse(Message):
	id: int = Field(..., title="Message ID")

class MessageListResponse(BaseModel):
    msgs: List[MessageResponse] = []
