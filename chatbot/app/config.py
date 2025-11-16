from dotenv import load_dotenv, find_dotenv
import os
from pathlib import Path

_ENV_PATH = find_dotenv()
load_dotenv(_ENV_PATH)

_gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if _gac and not os.path.isabs(_gac):
    env_dir = Path(_ENV_PATH).parent if _ENV_PATH else Path.cwd()
    abs_gac = str((env_dir / _gac).resolve())
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = abs_gac

LLM_API_BASE = os.getenv("LLM_API_BASE", "http://localhost:11434/v1")
LLM_API_KEY  = os.getenv("LLM_API_KEY", "not-used")
LLM_MODEL    = os.getenv("LLM_MODEL", "llama3.1")

PROJECT_ID       = os.getenv("B311_PROJECT", "boston311-mlops")
RAW_DATASET      = os.getenv("B311_RAW_DATASET", "boston311")
RAW_TABLE        = os.getenv("B311_RAW_TABLE", "service_requests_2025")
SERVING_DATASET  = os.getenv("B311_SERVING_DATASET", "boston311_service")
BQ_LOCATION      = os.getenv("B311_BQ_LOCATION", "US")

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