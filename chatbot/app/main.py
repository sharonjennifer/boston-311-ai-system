from fastapi import FastAPI
from app.schemas import ChatRequest, ChatResponse
from app.pipeline import run_pipeline
from query_parser.query_parser import get_parser
from rag.vector_store import get_retriever

app = FastAPI(title="Boston 311 Chatbot Backend")

@app.on_event("startup")
async def startup_event():
    get_parser()
    get_retriever()

@app.post("/chat", response_model=ChatResponse)
def chat_endpoint(request: ChatRequest):
    answer, sql, data = run_pipeline(request.question)
    return ChatResponse(answer=answer, sql=sql, data=data)
