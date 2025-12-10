from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.pipeline import run_pipeline
from app.schemas import ChatRequest, ChatResponse
import uuid
from app.conversation_manager import get_conversation_manager
from app.monitoring import MonitoringMiddleware

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
    from rag.vector_store import get_retriever
    from app.faq_retriever import get_faq_retriever
    
    get_retriever(force_rebuild=False)
    get_faq_retriever(force_rebuild=False)

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

@app.get("/cache/stats")
def get_cache_stats():
    """Get cache statistics"""
    from app.query_cache import get_query_cache
    cache = get_query_cache()
    return cache.get_stats()

@app.post("/cache/clear")
def clear_cache():
    """Clear the query cache"""
    from app.query_cache import get_query_cache
    cache = get_query_cache()
    cache.clear()
    return {"message": "Cache cleared successfully"}

@app.get("/cache/queries")
def get_cached_queries():
    """See what queries are currently cached"""
    from app.query_cache import get_query_cache
    cache = get_query_cache()
    return {"cached_queries": cache.get_cached_queries()}

@app.get("/admin/metrics")
def get_admin_metrics():
    """Get all monitoring metrics"""
    from app.monitoring import get_metrics_collector
    metrics = get_metrics_collector()
    return metrics.get_summary_stats()

@app.get("/admin/popular-questions")
def get_popular_questions():
    """Get most frequently asked questions"""
    from app.monitoring import get_metrics_collector
    metrics = get_metrics_collector()
    return {"popular_questions": metrics.get_popular_questions(top_n=10)}

@app.get("/admin/recent-requests")
def get_recent_requests(n: int = 50):
    """Get recent requests"""
    from app.monitoring import get_metrics_collector
    metrics = get_metrics_collector()
    return {"recent_requests": metrics.get_recent_requests(n=n)}

@app.get("/admin/slow-queries")
def get_slow_queries():
    """Get slow queries"""
    from app.monitoring import get_metrics_collector
    metrics = get_metrics_collector()
    return {"slow_queries": metrics.get_slow_queries()}

@app.get("/admin/dashboard")
def admin_dashboard():
    """Serve monitoring dashboard HTML"""
    from fastapi.responses import FileResponse
    from pathlib import Path
    dashboard_path = Path(__file__).parent.parent / "templates" / "dashboard.html"
    return FileResponse(dashboard_path)