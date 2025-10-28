import os
import time
import logging
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_URL     = os.getenv("BOSTON311_BASE_URL", "https://data.boston.gov/api/3/action/datastore_search_sql")
RESOURCE_ID  = os.getenv("BOSTON311_RESOURCE_ID", "9d7c2214-4709-478a-a2e8-fb2020a5bb94")
PAGE_SIZE    = int(os.getenv("BOSTON311_PAGE_SIZE", "5000"))
TIMEOUT      = 30
MAX_RETRIES  = 3
BACKOFF_SECS = 2
HEADERS      = {"User-Agent": "boston311-composer/1.0"}

def _do_request(params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise RuntimeError(f"CKAN returned success=False: {data}")
            return data["result"].get("records", [])
        except Exception as e:
            logger.warning("Request failed (attempt %s/%s): %s", attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                logger.error("Giving up after %s attempts", MAX_RETRIES)
                raise
            time.sleep(BACKOFF_SECS * attempt)

def fetch_window(where_sql: str, start_after_id: int = 0, limit: int = PAGE_SIZE):
    clauses = []
    if where_sql and where_sql.strip() and where_sql.strip() != "1=1":
        clauses.append(f"({where_sql})")
    clauses.append(f"_id > {start_after_id}")

    where = " AND ".join(clauses)
    
    sql = (
        f'SELECT * FROM "{RESOURCE_ID}" '
        f"WHERE {where} "
        f"ORDER BY _id "
        f"LIMIT {limit}"
    )

    logger.debug("SQL start_after_id: %s", str(start_after_id))
    return _do_request({"sql": sql})