# app/main.py

import logging, os

from pathlib import Path
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv, find_dotenv

from app.bq import dry_run, run
from app.sql_templates import TEMPLATES
from app.router_gemini import choose_and_fill



load_dotenv(find_dotenv())

logging.basicConfig(
    level=os.getenv("B311_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logging.getLogger("google").setLevel(logging.WARNING)
logging.getLogger("grpc").setLevel(logging.WARNING)
logging.getLogger("absl").setLevel(logging.WARNING)
logger = logging.getLogger("b311.main")

CORPUS_DIR = Path(__file__).resolve().parents[1] / "corpus"
schema_snippet = (CORPUS_DIR / "schema_cards.md").read_text() if (CORPUS_DIR / "schema_cards.md").exists() else ""
rules_snippet  = (CORPUS_DIR / "rules.md").read_text() if (CORPUS_DIR / "rules.md").exists() else ""



app = FastAPI(title="Boston 311 Chatbot")

class Ask(BaseModel):
    question: str

@app.post("/query")
def query(payload: Ask):
    logger.info("POST /query question=%r", payload.question)

    choice = choose_and_fill(payload.question, schema_snippet, rules_snippet)
    if choice.get("needs_clarification"):
        logger.info("Needs clarification: %s", choice.get("clarify_question"))
        return {"needs_clarification": True, "clarify_question": choice.get("clarify_question","")}

    tid = choice.get("template_id")
    params = choice.get("params", {})
    tmpl = TEMPLATES.get(tid)
    if not tmpl:
        logger.error("No template for %s", tid)
        raise HTTPException(status_code=400, detail=f"No template for {tid}")
    sql = tmpl["sql"]

    logger.info("Executing template=%s params=%s", tid, params)
    logger.debug("SQL:\n%s", sql)

    # DRY-RUN
    try:
        dry = dry_run(sql, params)
        if dry["total_bytes_processed"] > 5 * 10**9:
            logger.warning("Query too large (bytes=%s)", dry["total_bytes_processed"])
            raise HTTPException(status_code=413, detail="Query too large; narrow time or filters.")
    except Exception as e:
        logger.exception("Dry-run failed")
        raise HTTPException(status_code=400, detail=f"Dry-run failed: {e}")

    # EXECUTE
    try:
        rows = run(sql, params)
        logger.info("Success: rows=%d (returning up to 50)", len(rows))
        return {
            "template_id": tid,
            "confidence": choice.get("confidence", None),
            "params": params,
            "rows": rows[:50],
            "meta": {"dry_run_bytes": dry["total_bytes_processed"]}
        }
    except Exception as e:
        logger.exception("Query failed")
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

@app.get("/healthz")
def healthz():
    return {"ok": True}
