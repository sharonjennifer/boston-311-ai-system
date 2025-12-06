from pydantic import BaseModel
from typing import Optional, List, Any

class ChatRequest(BaseModel):
    question: str

class ChatResponse(BaseModel):
    answer: str
    sql: Optional[str] = None
    data: Optional[List[Any]] = None
