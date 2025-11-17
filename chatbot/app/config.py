# app/config.py
 
import os

from pathlib import Path
from dotenv import load_dotenv, find_dotenv



ENV_PATH = find_dotenv()
load_dotenv(ENV_PATH)

gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if gac and not os.path.isabs(gac):
    env_dir = Path(ENV_PATH).parent if ENV_PATH else Path.cwd()
    abs_gac = str((env_dir / gac).resolve())
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = abs_gac

PROJECT_ID       = os.getenv("B311_PROJECT", "boston311-mlops")
RAW_DATASET      = os.getenv("B311_RAW_DATASET", "boston311")
RAW_TABLE        = os.getenv("B311_RAW_TABLE", "service_requests_2025")
SERVING_DATASET  = os.getenv("B311_SERVING_DATASET", "boston311_service")
BQ_LOCATION      = os.getenv("B311_BQ_LOCATION", "US")

VERTEX_PROJECT  = os.getenv("VERTEX_PROJECT", PROJECT_ID)
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")
MODEL_ID        = os.getenv("VERTEX_MODEL",  "gemini-2.5-flash") 

FULL_RAW     = f"`{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}`"
REF_NEI      = f"`{PROJECT_ID}.{SERVING_DATASET}.ref_neighborhoods`"
TBL_NEI_WEEK = f"`{PROJECT_ID}.{SERVING_DATASET}.tbl_counts_by_neighborhood_week`"
TBL_DEPT_WEEK= f"`{PROJECT_ID}.{SERVING_DATASET}.tbl_counts_by_department_week`"
TBL_DUR      = f"`{PROJECT_ID}.{SERVING_DATASET}.tbl_case_durations`"

ALLOWLIST = {
    FULL_RAW.strip("`"),
    REF_NEI.strip("`"),
    TBL_NEI_WEEK.strip("`"),
    TBL_DEPT_WEEK.strip("`"),
    TBL_DUR.strip("`"),
}

DEFAULTS = {
    "count_days": 30,
    "trend_weeks": 12,
    "topn_weeks": 8,
    "limit": 50,
    "tz": "America/New_York",
}

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")