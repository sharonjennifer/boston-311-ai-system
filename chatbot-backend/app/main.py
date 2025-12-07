from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.pipeline import run_pipeline
from app.schemas import ChatRequest, ChatResponse

app = FastAPI(
    title="Boston 311 SQL Chatbot",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_event():
    from rag.vector_store import get_retriever
    get_retriever(force_rebuild=False)


@app.post("/chat", response_model=ChatResponse)
def chat_endpoint(request: ChatRequest):
    answer, sql, data = run_pipeline(request.question)
    return ChatResponse(answer=answer, sql=sql, data=data)