import os
import time
import requests

BASE_URL   = os.getenv("BOSTON311_BASE_URL", "https://data.boston.gov/api/3/action/datastore_search_sql")
RESOURCE_ID = os.getenv("BOSTON311_RESOURCE_ID", "9d7c2214-4709-478a-a2e8-fb2020a5bb94")
PAGE_SIZE   = int(os.getenv("BOSTON311_PAGE_SIZE", "5000"))
TIMEOUT     = 30
MAX_RETRIES = 3
BACKOFF_SECS= 2
HEADERS     = {"User-Agent": "boston311-composer/1.0"}

def fetch_page(last_id, limit = PAGE_SIZE):
    sql = (
        f'SELECT * FROM "{RESOURCE_ID}" '
        f"WHERE _id > {last_id} "
        f"ORDER BY _id "
        f"LIMIT {limit}"
    )
    params = {"sql": sql}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise RuntimeError(f"CKAN returned success=False: {data}")
            result = data["result"]
            return result.get("records", [])
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(BACKOFF_SECS * attempt)
    raise RuntimeError("Unreachable")