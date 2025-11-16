# app/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pathlib import Path
from app.router_llm import choose_and_fill
from app.sql_templates import TEMPLATES
from app.bq import dry_run, run

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

CORPUS_DIR = Path(__file__).resolve().parents[1] / "corpus"
_schema_snippet = (CORPUS_DIR / "schema_cards.md").read_text() if (CORPUS_DIR / "schema_cards.md").exists() else ""
_rules_snippet  = (CORPUS_DIR / "rules.md").read_text()        if (CORPUS_DIR / "rules.md").exists() else ""

app = FastAPI(title="Boston 311 Chatbot")

class Ask(BaseModel):
    question: str

@app.post("/query")
def query(payload: Ask):
    # 1) LLM router
    choice = choose_and_fill(payload.question, _schema_snippet, _rules_snippet)
    if choice.get("needs_clarification"):
        return {"needs_clarification": True, "clarify_question": choice.get("clarify_question","")}

    tid = choice.get("template_id")
    params = choice.get("params", {})
    tmpl = TEMPLATES.get(tid)
    if not tmpl:
        raise HTTPException(status_code=400, detail=f"No template for {tid}")
    sql = tmpl["sql"]

    # 2) DRY RUN guard
    try:
        dry = dry_run(sql, params)
        if dry["total_bytes_processed"] > 5 * 10**9:
            raise HTTPException(status_code=413, detail="Query too large; narrow time or filters.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Dry-run failed: {e}")

    # 3) Execute
    try:
        rows = run(sql, params)
        return {
            "template_id": tid,
            "confidence": choice.get("confidence", None),
            "params": params,
            "rows": rows[:50],
            "meta": {"dry_run_bytes": dry["total_bytes_processed"]}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

@app.get("/healthz")
def healthz():
    return {"ok": True}
