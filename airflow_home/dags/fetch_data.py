import argparse
import time
import requests

BASE_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"
RESOURCE_ID = "9d7c2214-4709-478a-a2e8-fb2020a5bb94"
PAGE_SIZE = 5000
TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_SECS = 2

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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--since",
        type=int,
        required=True, 
        default=0,
    )
    args = parser.parse_args()
    
    if args.since < 0:
        raise SystemExit("--since must be a non-negative integer.")
    
    last_id = args.since
    total = 0
    
    while True:
        records = fetch_page(last_id)
        if not records:
            break
        
        total += len(records)

        last_id = records[-1].get("_id", last_id)
        
        if len(records) < PAGE_SIZE:
            break
        if total > 50000:
            break
    
    print(f"Fetched {total} records after _id > {args.since}.")
    
if __name__ == "__main__":
    main()