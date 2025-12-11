from __future__ import annotations

import time
import threading

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

@dataclass
class RequestEvent:
    ts: float
    path: str
    method: str
    status_code: int
    latency_ms: float
    error: Optional[str] = None


@dataclass
class StepEvent:
    ts: float
    step: str
    latency_ms: float
    success: bool
    error: Optional[str] = None


class MetricsCollector:
    
    def __init__(self, max_request_events = 1000, max_step_events = 2000):
        self._events = deque(maxlen=max_request_events)
        self._step_events = deque(maxlen=max_step_events)
        self._lock = threading.Lock()

    def record_request(self, event):
        with self._lock:
            self._events.append(event)

    def record_step(self, event):
        with self._lock:
            self._step_events.append(event)

    def _iso(self, ts):
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    def _p95(self, values):
        if not values:
            return 0.0
        values_sorted = sorted(values)
        n = len(values_sorted)
        idx = max(int(n * 0.95) - 1, 0)
        return values_sorted[idx]

    def _snapshot_requests(self):
        with self._lock:
            events = list(self._events)

        total = len(events)
        if not total:
            return {
                "summary": {
                    "total_requests": 0,
                    "avg_latency_ms": 0.0,
                    "p95_latency_ms": 0.0,
                    "error_rate": 0.0,
                    "last_error": None,
                },
                "by_path": [],
                "recent": [],
            }

        latencies = [e.latency_ms for e in events]
        avg_latency = sum(latencies) / total
        p95_latency = self._p95(latencies)

        error_events = [e for e in events if e.status_code >= 400]
        error_rate = len(error_events) / total

        last_error = None
        if error_events:
            last = error_events[-1]
            last_error = {
                "time": self._iso(last.ts),
                "path": last.path,
                "status_code": last.status_code,
                "message": last.error or "",
            }

        path_stats = defaultdict(
            lambda: {"path": "", "count": 0, "_latencies": [], "_errors": 0}
        )
        for e in events:
            s = path_stats[e.path]
            s["path"] = e.path
            s["count"] += 1
            s["_latencies"].append(e.latency_ms)
            if e.status_code >= 400:
                s["_errors"] += 1

        by_path = []
        for p, s in path_stats.items():
            count = s["count"]
            errors = s["_errors"]
            lat_list = s["_latencies"]
            by_path.append({
                "path": p,
                "count": count,
                "avg_latency_ms": sum(lat_list) / count if count else 0.0,
                "error_rate": errors / count if count else 0.0,
            })

        by_path.sort(key=lambda x: x["count"], reverse=True)

        recent_events = events[-50:]
        recent = [
            {
                "time": self._iso(e.ts),
                "path": e.path,
                "method": e.method,
                "status_code": e.status_code,
                "latency_ms": round(e.latency_ms, 1),
                "error": e.error,
            }
            for e in reversed(recent_events)
        ]

        return {
            "summary": {
                "total_requests": total,
                "avg_latency_ms": round(avg_latency, 1),
                "p95_latency_ms": round(p95_latency, 1),
                "error_rate": round(error_rate, 3),
                "last_error": last_error,
            },
            "by_path": by_path,
            "recent": recent,
        }

    def _snapshot_steps(self):
        with self._lock:
            events = list(self._step_events)

        if not events:
            return {
                "steps_by_name": [],
                "steps_recent": [],
            }

        step_stats = defaultdict(
            lambda: {"step": "", "count": 0, "_latencies": [], "_errors": 0}
        )
        for e in events:
            s = step_stats[e.step]
            s["step"] = e.step
            s["count"] += 1
            s["_latencies"].append(e.latency_ms)
            if not e.success:
                s["_errors"] += 1

        steps_by_name = []
        for step, s in step_stats.items():
            count = s["count"]
            lat_list = s["_latencies"]
            errors = s["_errors"]
            steps_by_name.append({
                "step": step,
                "count": count,
                "avg_latency_ms": sum(lat_list) / count if count else 0.0,
                "p95_latency_ms": self._p95(lat_list) if count else 0.0,
                "error_rate": errors / count if count else 0.0,
            })

        steps_by_name.sort(key=lambda x: x["step"])
        recent_events = events[-50:]
        steps_recent = [
            {
                "time": self._iso(e.ts),
                "step": e.step,
                "latency_ms": round(e.latency_ms, 1),
                "success": e.success,
                "error": e.error,
            }
            for e in reversed(recent_events)
        ]

        return {
            "steps_by_name": steps_by_name,
            "steps_recent": steps_recent,
        }

    def snapshot(self):
        req = self._snapshot_requests()
        steps = self._snapshot_steps()
        return {**req, **steps}

metrics_collector = MetricsCollector()


def get_metrics_snapshot():
    return metrics_collector.snapshot()


def record_step_timing(step, latency_ms, success, error = None):
    metrics_collector.record_step(
        StepEvent(
            ts=time.time(),
            step=step,
            latency_ms=latency_ms,
            success=success,
            error=error,
        )
    )

class MonitoringMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        
        path = request.url.path
        if path.startswith("/monitor"):
            return await call_next(request)
        
        start = time.perf_counter()
        error_msg = None
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception as exc:
            error_msg = repr(exc)
            status_code = 500
            raise
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            metrics_collector.record_request(
                RequestEvent(
                    ts=time.time(),
                    path=str(request.url.path),
                    method=request.method,
                    status_code=status_code,
                    latency_ms=latency_ms,
                    error=error_msg,
                )
            )
