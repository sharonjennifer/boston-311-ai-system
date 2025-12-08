from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.pipeline import run_pipeline
from app.schemas import ChatRequest, ChatResponse
import uuid
from app.conversation_manager import get_conversation_manager

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
    # Generate session ID if not provided
    session_id = request.session_id or str(uuid.uuid4())
    
    answer, sql, data = run_pipeline(request.question, session_id=session_id)
    
    return ChatResponse(
        answer=answer,
        sql=sql,
        data=data,
        session_id=session_id
    )

@app.post("/session/new")
def create_session():
    session_id = str(uuid.uuid4())
    return {"session_id": session_id, "message": "New session created"}

@app.delete("/session/{session_id}")
def clear_session(session_id: str):
    conv_manager = get_conversation_manager()
    conv_manager.clear_session(session_id)
    return {"session_id": session_id, "message": "Session cleared"}

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "Boston 311 Chatbot"}