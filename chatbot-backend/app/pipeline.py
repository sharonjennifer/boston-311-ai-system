"""
Enhanced Pipeline with Intelligent Routing, Caching, and Monitoring
"""
import os
import sys
import logging
from pathlib import Path
import json

from dotenv import load_dotenv
from google.cloud import bigquery

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
sys.path.append(str(PROJECT_ROOT))

from query_parser.query_parser import parse_query
from rag.vector_store import retrieve_keywords
from sql_model.endpoint import generate_sql
from answer_model.endpoint import generate_answer
from app.config import settings

from typing import Tuple, List, Dict, Optional
from app.conversation_manager import get_conversation_manager
from app.routers import route_question, handle_procedural_question

load_dotenv()

LOG_LEVEL = os.getenv("B311_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("b311.pipeline")

bq = bigquery.Client(project=settings.PROJECT_ID)


def run_pipeline(question: str, session_id: Optional[str] = None):
    """
    Enhanced pipeline with intelligent routing, caching, and monitoring
    
    Flow:
    1. Route question (topic relevance + query type)
    2. If off-topic: return polite message
    3. If procedural: return FAQ answer
    4. If data query: proceed with SQL generation
    """
    sql_query = None
    records = []
    cached_records = None
    
    # Get conversation manager
    conv_manager = get_conversation_manager()
    
    # Get conversation context if session exists
    context = ""
    if session_id:
        context = conv_manager.get_context(session_id, num_turns=2)
    
    # ========================================================================
    # ROUTING LAYER
    # ========================================================================
    route_type, route_metadata = route_question(question)
    
    # Handle OFF-TOPIC questions
    if route_type == "off_topic":
        off_topic_message = route_metadata.get("message", 
            "I can only help with Boston 311 service requests.")
        
        # Track metrics
        try:
            from app.monitoring import get_metrics_collector
            get_metrics_collector().requests_by_route_type["off_topic"] += 1
        except: 
            pass
        
        if session_id:
            conv_manager.add_turn(session_id, question, off_topic_message, None, None)
        
        return off_topic_message, None, []
    
    # Handle PROCEDURAL questions (FAQ)
    if route_type == "procedural":
        logger.info("[Pipeline] Routing to FAQ handler")
        procedural_answer = handle_procedural_question(question)
        
        # Track metrics
        try:
            from app.monitoring import get_metrics_collector
            get_metrics_collector().requests_by_route_type["procedural"] += 1
        except: 
            pass
        
        if session_id:
            conv_manager.add_turn(session_id, question, procedural_answer, None, None)
        
        return procedural_answer, None, []
    
    # ========================================================================
    # DATA QUERY PIPELINE
    # ========================================================================
    logger.info("[Pipeline] Routing to data query pipeline")
    
    # Track metrics for data query
    try:
        from app.monitoring import get_metrics_collector
        get_metrics_collector().requests_by_route_type["data_query"] += 1
    except: 
        pass

    # 1. Parse user question with context
    try:
        parsed = parse_query(question, context=context)
        logger.info("Parsed output %s", parsed)
    except Exception as e:
        logger.exception("Query parsing failed")
        error_msg = (
            "Sorry, I couldn't understand that question well enough to build a query. "
            "Try rephrasing it or asking something a bit simpler."
        )
        
        if session_id:
            conv_manager.add_turn(session_id, question, error_msg, None, None)
        
        return error_msg, None, []

    # 2. Retrieve RAG keywords (non-fatal if it fails)
    try:
        keywords = retrieve_keywords(parsed)
        logger.info("Keywords output %s", keywords)
    except Exception as e:
        logger.exception("Keyword retrieval failed; continuing without RAG hints.")
        keywords = []

    # 3. Generate SQL via Vertex endpoint with context
    try:
        sql_query = generate_sql(question, keywords, context=context)
        logger.info("SQL Query output %s", sql_query)

        if not sql_query or not sql_query.strip():
            raise ValueError("Empty SQL from generate_sql")
    except Exception as e:
        logger.exception("SQL generation failed")
        error_msg = (
            "I couldn't generate a valid SQL query for that question. "
            "You can try rephrasing, or narrowing down things like date range or neighborhood."
        )
        
        if session_id:
            conv_manager.add_turn(session_id, question, error_msg, None, None)
        
        return error_msg, None, []

    # 4. Check cache before running query
    from app.query_cache import get_query_cache
    cache = get_query_cache()
    
    cached_records = cache.get(sql_query)
    if cached_records is not None:
        records = cached_records
        logger.info(f"Using cached results ({len(records)} rows)")
    else:
        # 4a. Run SQL on BigQuery
        try:
            df = bq.query(sql_query).to_dataframe()
            logger.info("BigQuery output %s", df)
        except Exception as e:
            logger.exception("BigQuery execution failed")
            error_msg = (
                "I tried to run the query in BigQuery, but something went wrong on the data side. "
                "Please try again in a moment or ask a simpler question."
            )
            
            if session_id:
                conv_manager.add_turn(session_id, question, error_msg, sql_query, None)
            
            return error_msg, sql_query, []

        # Convert to JSON-serializable format
        records = json.loads(df.to_json(orient="records"))
        
        # Cache the results
        cache.set(sql_query, records)
        logger.info(f"Cached query results ({len(records)} rows)")

    # 5. Explicit "no data found" case
    if not records:
        logger.info("BigQuery returned 0 rows.")
        no_data_msg = (
            "I ran the query, but there were no matching results for your question."
        )
        
        if session_id:
            conv_manager.add_turn(session_id, question, no_data_msg, sql_query, records)
        
        return no_data_msg, sql_query, []

    # 6. Turn results into a natural-language answer (Gemini)
    try:
        answer = generate_answer(question, sql_query, records)
        logger.info("User output %s", answer)
    except Exception as e:
        logger.exception("Answer generation failed; falling back to simple formatting.")
        first = records[0]
        parts = ", ".join(f"{k}: {v}" for k, v in first.items())
        answer = "Here are the results: " + parts

    # Store conversation turn if session exists
    if session_id:
        conv_manager.add_turn(session_id, question, answer, sql_query, records)
    
    return answer, sql_query, records