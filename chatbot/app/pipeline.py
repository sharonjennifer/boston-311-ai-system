import os
import sys 
import logging

from dotenv import load_dotenv
from pathlib import Path
from google.cloud import bigquery

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
sys.path.append(str(PROJECT_ROOT))

from query_parser.query_parser import parse_query
from rag.vector_store import retrieve_keywords
from sql_model.endpoint import generate_sql
from app.config import settings

LOG_LEVEL = os.getenv("B311_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("b311.pipeline")


bq = bigquery.Client(project=settings.PROJECT_ID)

def run_pipeline(question):
    # 1. Query parsing
    parsed = parse_query(question)
    logger.info("Parsed output %s", parsed)

    # 2. Keyword retrieval
    keywords = retrieve_keywords(parsed)
    logger.info("Keywords output %s", keywords)

    # 3. SQL generation via Vertex
    sql_query = generate_sql(question, keywords)
    logger.info("SQL Query output %s", sql_query)

    # 4. BigQuery execution
    results = bq.query(sql_query).to_dataframe()
    logger.info("BigQuery output %s", results)

    # 5. Convert results to natural language
    answer = dataframe_to_text(results)
    logger.info("User output %s", answer)

    return answer, sql_query, results.to_dict(orient="records")


def dataframe_to_text(df):
    if df.empty:
        return "No results found for your query."
    
    first_row = df.iloc[0].to_dict()
    parts = [f"{k}: {v}" for k, v in first_row.items()]
    return "Here are the results: " + ", ".join(parts)
