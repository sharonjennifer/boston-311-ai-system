"""
BigQuery Metrics Writer

Batches metrics and writes to BigQuery periodically (zero extra cost)
"""
import os
import logging
import hashlib
from datetime import datetime
from typing import List, Dict
from google.cloud import bigquery
from threading import Thread, Lock
import time

logger = logging.getLogger("b311.metrics_writer")

PROJECT_ID = os.getenv("B311_PROJECT_ID")
METRICS_TABLE = f"{PROJECT_ID}.boston311.chatbot_metrics"
BATCH_SIZE = 50  # Write to BQ every 50 requests
BATCH_INTERVAL = 300  # Or every 5 minutes, whichever comes first


class MetricsWriter:
    """Batch writes metrics to BigQuery"""
    
    def __init__(self):
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.batch = []
        self.lock = Lock()
        self.last_write = time.time()
        logger.info(f"MetricsWriter initialized (table: {METRICS_TABLE})")
    
    def add_metric(
        self,
        request_id: str,
        endpoint: str,
        question: str,
        route_type: str,
        is_cached: bool,
        sql_query: str,
        latency_ms: float,
        status_code: int,
        result_count: int,
        error_type: str = None,
        error_message: str = None,
        session_id: str = None
    ):
        """Add metric to batch"""
        
        # Generate SQL hash for deduplication
        sql_hash = None
        if sql_query:
            sql_hash = hashlib.md5(sql_query.encode()).hexdigest()[:16]
        
        metric = {
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat(),
            "session_id": session_id,
            "endpoint": endpoint,
            "question": question[:500] if question else None,
            "question_normalized": question.lower().strip()[:200] if question else None,
            "route_type": route_type,
            "is_cached": is_cached,
            "sql_query": sql_query[:1000] if sql_query else None,
            "sql_hash": sql_hash,
            "latency_ms": latency_ms,
            "status_code": status_code,
            "result_count": result_count,
            "error_type": error_type,
            "error_message": error_message[:500] if error_message else None,
            "user_agent": None,
            "ip_address": None
        }
        
        with self.lock:
            self.batch.append(metric)
            
            # Check if we should write
            should_write = (
                len(self.batch) >= BATCH_SIZE or
                time.time() - self.last_write > BATCH_INTERVAL
            )
            
            if should_write:
                self._write_batch()
    
    def _write_batch(self):
        """Write batch to BigQuery"""
        if not self.batch:
            return
        
        try:
            # Copy batch and clear
            batch_to_write = self.batch.copy()
            self.batch.clear()
            self.last_write = time.time()
            
            # Write to BigQuery
            errors = self.bq_client.insert_rows_json(
                METRICS_TABLE,
                batch_to_write
            )
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
            else:
                logger.info(f"Successfully wrote {len(batch_to_write)} metrics to BigQuery")
                
        except Exception as e:
            logger.error(f"Failed to write metrics to BigQuery: {e}")
            # Don't crash - just log the error
    
    def flush(self):
        """Manually flush remaining metrics"""
        with self.lock:
            if self.batch:
                self._write_batch()


# Global writer instance
_metrics_writer = None


def get_metrics_writer() -> MetricsWriter:
    """Get global metrics writer"""
    global _metrics_writer
    if _metrics_writer is None:
        _metrics_writer = MetricsWriter()
    return _metrics_writer