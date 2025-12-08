from pydantic import BaseModel
from typing import Optional, List, Any

class ChatRequest(BaseModel):
    question: str
    session_id: Optional[str] = None

class ChatResponse(BaseModel):
    answer: str
    sql: Optional[str] = None
    data: Optional[List[Any]] = None
    session_id: str