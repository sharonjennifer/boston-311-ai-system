"""
Zero-Cost Monitoring System

Tracks chatbot performance metrics using in-memory storage and BigQuery
No external services required - completely free
"""
import time
import json
import logging
from datetime import datetime
from typing import Optional
from collections import defaultdict, deque
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("b311.monitoring")


class MetricsCollector:
    """In-memory metrics collector with BigQuery batch writes"""
    
    def __init__(self, max_history: int = 1000):
        # Counters
        self.total_requests = 0
        self.total_errors = 0
        self.total_latency = 0.0
        
        # Breakdown by type
        self.requests_by_endpoint = defaultdict(int)
        self.requests_by_route_type = defaultdict(int)  # off_topic, procedural, data_query
        self.errors_by_type = defaultdict(int)
        
        # Recent history (for dashboard)
        self.recent_requests = deque(maxlen=max_history)
        self.slow_queries = deque(maxlen=100)  # Queries >5 seconds
        self.failed_queries = deque(maxlen=100)  # Failed queries
        
        # Popular questions
        self.question_frequency = defaultdict(int)
        
        logger.info(f"MetricsCollector initialized (max_history={max_history})")
    
    def record_request(
        self,
        endpoint: str,
        latency: float,
        status_code: int,
        question: Optional[str] = None,
        sql: Optional[str] = None,
        route_type: Optional[str] = None,
        error: Optional[str] = None,
        session_id: Optional[str] = None
    ):
        """Record a request with all its metrics"""
        self.total_requests += 1
        self.total_latency += latency
        self.requests_by_endpoint[endpoint] += 1
        
        if route_type:
            self.requests_by_route_type[route_type] += 1
        
        # Track errors
        if status_code >= 400:
            self.total_errors += 1
            if error:
                self.errors_by_type[error] += 1
        
        # Track question frequency
        if question:
            # Normalize question for counting
            normalized_q = question.lower().strip()[:100]
            self.question_frequency[normalized_q] += 1
        
        # Record full request details
        request_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "endpoint": endpoint,
            "latency_ms": round(latency * 1000, 2),
            "status_code": status_code,
            "question": question[:200] if question else None,
            "sql": sql[:200] if sql else None,
            "route_type": route_type,
            "error": error,
            "session_id": session_id
        }
        
        self.recent_requests.append(request_record)
        
        # Track slow queries
        if latency > 5.0 and sql:
            self.slow_queries.append({
                "timestamp": datetime.utcnow().isoformat(),
                "question": question,
                "latency_s": round(latency, 2),
                "sql": sql[:300] if sql else None
            })
        
        # Track failed queries
        if status_code >= 400 and question:
            self.failed_queries.append({
                "timestamp": datetime.utcnow().isoformat(),
                "question": question,
                "error": error,
                "sql": sql[:200] if sql else None
            })
    
    def get_summary_stats(self) -> dict:
        """Get summary statistics"""
        avg_latency = (self.total_latency / self.total_requests 
                      if self.total_requests > 0 else 0)
        error_rate = (self.total_errors / self.total_requests 
                     if self.total_requests > 0 else 0)
        
        return {
            "total_requests": self.total_requests,
            "total_errors": self.total_errors,
            "error_rate_percent": round(error_rate * 100, 2),
            "avg_latency_ms": round(avg_latency * 1000, 2),
            "requests_by_endpoint": dict(self.requests_by_endpoint),
            "requests_by_route_type": dict(self.requests_by_route_type),
            "errors_by_type": dict(self.errors_by_type)
        }
    
    def get_popular_questions(self, top_n: int = 10) -> list:
        """Get most frequently asked questions"""
        sorted_questions = sorted(
            self.question_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return [
            {"question": q, "count": count}
            for q, count in sorted_questions[:top_n]
        ]
    
    def get_recent_requests(self, n: int = 50) -> list:
        """Get recent requests"""
        return list(self.recent_requests)[-n:]
    
    def get_slow_queries(self) -> list:
        """Get slow queries"""
        return list(self.slow_queries)
    
    def get_failed_queries(self) -> list:
        """Get failed queries"""
        return list(self.failed_queries)


# Global metrics instance
_metrics = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector"""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector()
    return _metrics


class MonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware to automatically track all requests"""
    
    async def dispatch(self, request: Request, call_next):
        # Skip monitoring for monitoring endpoints
        if request.url.path.startswith("/admin") or request.url.path == "/metrics":
            return await call_next(request)
        
        start_time = time.time()
        
        # Extract request details
        question = None
        session_id = None
        
        if request.url.path == "/chat" and request.method == "POST":
            try:
                body = await request.body()
                body_json = json.loads(body)
                question = body_json.get("question")
                session_id = body_json.get("session_id")
            except:
                pass
        
        # Process request
        response = None
        error = None
        
        try:
            response = await call_next(request)
            return response
        except Exception as e:
            error = str(type(e).__name__)
            logger.exception(f"Request failed: {e}")
            raise
        finally:
            # Record metrics
            latency = time.time() - start_time
            status_code = response.status_code if response else 500
            
            metrics = get_metrics_collector()
            metrics.record_request(
                endpoint=request.url.path,
                latency=latency,
                status_code=status_code,
                question=question,
                sql=None,  # Will be populated by pipeline
                route_type=None,  # Will be populated by pipeline
                error=error,
                session_id=session_id
            )
            
            # Log request completion
            logger.info(
                f"Request completed: {request.method} {request.url.path} "
                f"- Status: {status_code} - Latency: {latency*1000:.0f}ms"
            )