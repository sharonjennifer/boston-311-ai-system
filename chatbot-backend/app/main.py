import sys

from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
sys.path.append(str(PROJECT_ROOT))

from app.pipeline import run_pipeline
from rag.vector_store import get_retriever
from router.faq_retriever import get_faq_retriever
from app.schemas import ChatRequest, ChatResponse
from cache_storage.query_cache import get_query_cache
from monitoring.monitoring import MonitoringMiddleware, get_metrics_snapshot

app = FastAPI(
    title="Boston 311 SQL Chatbot",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(MonitoringMiddleware)

@app.on_event("startup")
def startup_event():
    get_retriever(force_rebuild=False)
    get_faq_retriever(force_rebuild=False)

@app.post("/chat", response_model=ChatResponse)
def chat_endpoint(request: ChatRequest):
    answer, sql, data = run_pipeline(request.question)
    
    return ChatResponse(
        answer=answer,
        sql=sql,
        data=data
    )

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "Boston 311 Chatbot"}

@app.get("/cache/stats")
def get_cache_stats():
    cache = get_query_cache()
    return cache.get_stats()


@app.post("/cache/clear")
def clear_cache():
    cache = get_query_cache()
    cache.clear()
    return {"message": "Cache cleared successfully"}


@app.get("/cache/queries")
def get_cached_queries():
    cache = get_query_cache()
    return {"cached_queries": cache.get_cached_queries()}

@app.get("/monitor/metrics")
def monitor_metrics():
    return JSONResponse(get_metrics_snapshot())