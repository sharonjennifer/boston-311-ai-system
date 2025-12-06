import os
import sys
import logging
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
from app.config import settings

load_dotenv()

LOG_LEVEL = os.getenv("B311_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("b311.pipeline")

bq = bigquery.Client(project=settings.PROJECT_ID)


def run_pipeline(question: str):
    sql_query = None
    records = []

    # 1. Parse user question
    try:
        parsed = parse_query(question)
        logger.info("Parsed output %s", parsed)
    except Exception as e:
        logger.exception("Query parsing failed")
        return (
            "Sorry, I couldn't understand that question well enough to build a query. "
            "Try rephrasing it or asking something a bit simpler.",
            None,
            [],
        )

    # 2. Retrieve RAG keywords (non-fatal if it fails)
    try:
        keywords = retrieve_keywords(parsed)
        logger.info("Keywords output %s", keywords)
    except Exception as e:
        logger.exception("Keyword retrieval failed; continuing without RAG hints.")
        keywords = []

    # 3. Generate SQL via Vertex endpoint
    try:
        sql_query = generate_sql(question, keywords)
        logger.info("SQL Query output %s", sql_query)

        if not sql_query or not sql_query.strip():
            raise ValueError("Empty SQL from generate_sql")
    except Exception as e:
        logger.exception("SQL generation failed")
        return (
            "I couldn't generate a valid SQL query for that question. "
            "You can try rephrasing, or narrowing down things like date range or neighborhood.",
            None,
            [],
        )

    # 4. Run SQL on BigQuery
    try:
        df = bq.query(sql_query).to_dataframe()
        logger.info("BigQuery output %s", df)
    except Exception as e:
        logger.exception("BigQuery execution failed")
        return (
            "I tried to run the query in BigQuery, but something went wrong on the data side. "
            "Please try again in a moment or ask a simpler question.",
            sql_query,
            [],
        )

    records = df.to_dict(orient="records")

    # 4a. Explicit “no data found” case
    if not records:
        logger.info("BigQuery returned 0 rows.")
        return (
            "I ran the query, but there were no matching results for your question.",
            sql_query,
            [],
        )

    # 5. Turn results into a natural-language answer (Gemini)
    try:
        answer = generate_answer(question, sql_query, records)
        logger.info("User output %s", answer)
    except Exception as e:
        logger.exception("Answer generation failed; falling back to simple formatting.")
        first = records[0]
        parts = ", ".join(f"{k}: {v}" for k, v in first.items())
        answer = "Here are the results: " + parts

    return answer, sql_query, records
