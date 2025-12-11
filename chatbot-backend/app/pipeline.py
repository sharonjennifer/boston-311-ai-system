import os
import sys
import logging
import json
import time

from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
sys.path.append(str(PROJECT_ROOT))

from query_parser.query_parser import parse_query
from rag.vector_store import retrieve_keywords
from sql_model.endpoint import generate_sql
from answer_model.endpoint import generate_answer
from router.routers import route_question, handle_procedural_question
from cache_storage.query_cache import get_query_cache
from monitoring.monitoring import record_step_timing
from app.config import settings

load_dotenv()

LOG_LEVEL = os.getenv("B311_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("b311.pipeline")

bq = bigquery.Client(project=settings.PROJECT_ID)



def run_pipeline(question):
    sql_query = None
    records = []
    cached_records = None

    # 1. Router (classify question)
    t0 = time.perf_counter()
    route_error = None
    try:
        route_type, route_metadata = route_question(question)
    except Exception as e:
        route_error = repr(e)
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("router", duration_ms, success=False, error=route_error)
        raise
    else:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("router", duration_ms, success=True)

    if route_type == "off_topic":
        off_topic_message = route_metadata.get(
            "message",
            "I can only help with Boston 311 service requests.",
        )
        return off_topic_message, None, []

    if route_type == "procedural":
        t0 = time.perf_counter()
        faq_err = None
        try:
            procedural_answer = handle_procedural_question(question)
        except Exception as e:
            faq_err = repr(e)
            duration_ms = (time.perf_counter() - t0) * 1000.0
            record_step_timing("faq_handler", duration_ms, success=False, error=faq_err)
            raise
        else:
            duration_ms = (time.perf_counter() - t0) * 1000.0
            record_step_timing("faq_handler", duration_ms, success=True)
            return procedural_answer, None, []

    # 2. Parse user question
    try:
        t0 = time.perf_counter()
        parsed = parse_query(question)
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("parse_query", duration_ms, success=True)
        logger.info("Parsed output %s", parsed)
    except Exception as e:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("parse_query", duration_ms, success=False, error=repr(e))
        logger.exception("Query parsing failed")
        error_msg = (
            "Sorry, I couldn't understand that question well enough to build a query. "
            "Try rephrasing it or asking something a bit simpler."
        )
        return error_msg, None, []

    # 3. Retrieve RAG keywords (non-fatal)
    try:
        t0 = time.perf_counter()
        keywords = retrieve_keywords(parsed)
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("rag_keywords", duration_ms, success=True)
        logger.info("Keywords output %s", keywords)
    except Exception as e:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("rag_keywords", duration_ms, success=False, error=repr(e))
        logger.exception("Keyword retrieval failed; continuing without RAG hints.")
        keywords = []

    # 4. Generate SQL
    try:
        t0 = time.perf_counter()
        sql_query = generate_sql(question, keywords)
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("generate_sql", duration_ms, success=True)
        logger.info("SQL Query output %s", sql_query)

        if not sql_query or not sql_query.strip():
            raise ValueError("Empty SQL from generate_sql")
    except Exception as e:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("generate_sql", duration_ms, success=False, error=repr(e))
        logger.exception("SQL generation failed")
        error_msg = (
            "I couldn't generate a valid SQL query for that question. "
            "You can try rephrasing, or narrowing down things like date range or neighborhood."
        )
        return error_msg, None, []

    # 5. Cache + BigQuery
    cache = get_query_cache()
    cached_records = cache.get(sql_query)

    if cached_records is not None:
        records = cached_records
        logger.info(f"Using cached results ({len(records)} rows)")
        record_step_timing("cache_lookup", latency_ms=0.1, success=True)
    else:
        t0 = time.perf_counter()
        try:
            df = bq.query(sql_query).to_dataframe()
            duration_ms = (time.perf_counter() - t0) * 1000.0
            record_step_timing("bigquery_query", duration_ms, success=True)
            logger.info("BigQuery output %s", df)
        except Exception as e:
            duration_ms = (time.perf_counter() - t0) * 1000.0
            record_step_timing("bigquery_query", duration_ms, success=False, error=repr(e))
            logger.exception("BigQuery execution failed")
            error_msg = (
                "I tried to run the query in BigQuery, but something went wrong on the data side. "
                "Please try again in a moment or ask a simpler question."
            )
            return error_msg, sql_query, []

        records = json.loads(df.to_json(orient="records"))
        cache.set(sql_query, records)
        logger.info(f"Cached query results ({len(records)} rows)")

    if not records:
        logger.info("BigQuery returned 0 rows.")
        no_data_msg = "I ran the query, but there were no matching results for your question."
        return no_data_msg, sql_query, []

    # 6. Answer generation
    try:
        t0 = time.perf_counter()
        answer = generate_answer(question, sql_query, records)
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("generate_answer", duration_ms, success=True)
        logger.info("User output %s", answer)
    except Exception as e:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        record_step_timing("generate_answer", duration_ms, success=False, error=repr(e))
        logger.exception("Answer generation failed; falling back to simple formatting.")
        first = records[0]
        parts = ", ".join(f"{k}: {v}" for k, v in first.items())
        answer = "Here are the results: " + parts

    return answer, sql_query, records
