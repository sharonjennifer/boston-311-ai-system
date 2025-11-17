# app/router_gemini.py

import pytz, vertexai
import json, re, os, pathlib, logging

from pathlib import Path
from datetime import datetime
from dateutil import parser as dtparser
from typing import List, Tuple, Dict, Any, Optional
from vertexai.generative_models import GenerativeModel, GenerationConfig

from app.retrieval import top_k
from app.config import VERTEX_PROJECT, VERTEX_LOCATION, MODEL_ID



logger = logging.getLogger("b311.router")

RAW_LOG_PATH = os.getenv("B311_RAW_LLM_LOG", "")
CORPUS_DIR = Path(__file__).resolve().parents[1] / "corpus"
TEMPLATES_JSONL = CORPUS_DIR / "templates.jsonl"
FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

boston_tz = pytz.timezone("America/New_York")
current_time = datetime.now(boston_tz)
current_time_iso = current_time.isoformat()
timezone_name = str(boston_tz)

PROMPT_SYSTEM = f"""You are a Boston 311 SQL router. You MUST select exactly one template from the provided candidates
and fill its parameters. Only use the schema and rules given. Output STRICT JSON with keys: 
{{"template_id": "...", "params": {{...}}, "confidence": 0..1, "needs_clarification": false, "clarify_question": ""}}

Constraints:
- The current time is {current_time_iso} ({timezone_name}). Use this to resolve all relative time queries (e.g., "yesterday", "this morning").
- Use ISO8601 timestamps (e.g., 2025-11-10T08:00:00-05:00) for any datetime parameters (assume {timezone_name} if user gives relative time).
- If a required parameter is missing or ambiguous, set needs_clarification=true and ask ONE short clarifying question.
- Do not invent parameters outside the chosen template.
- Do not expand user time ranges beyond what was requested. Minimal defaults are OK if user didn't specify.
- Output JSON ONLY. No prose, no code blocks, no extra text.
"""

vertexai.init(project=VERTEX_PROJECT, location=VERTEX_LOCATION)
model = GenerativeModel(MODEL_ID, system_instruction=[PROMPT_SYSTEM])
logger.info("Vertex model ready")

TEMPLATE_REGISTRY = {}
try:
    if TEMPLATES_JSONL.exists():
        with open(TEMPLATES_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    obj = json.loads(line)
                    TEMPLATE_REGISTRY[obj["id"]] = obj
    else:
        logger.warning("templates.jsonl not found at %s", TEMPLATES_JSONL)
except Exception as e:
    logger.error("Failed to load template registry: %s", e)



def log_llm_raw(label:str, text: str):
    logger.info("LLM input prompt stored in raw log")
    
    if RAW_LOG_PATH:
        try:
            p = pathlib.Path(RAW_LOG_PATH)
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("a", encoding="utf-8") as f:
                f.write(f"\n===== {label} =====\n")
                f.write(text or "")
                f.write("\n")
        except Exception as e:
            logger.warning("Could not write raw LLM log to %s: %s", RAW_LOG_PATH, e)


def pretty_candidates(cands: List[Tuple[str, float]]):
    lines = []
    for tid, score in cands:
        meta = TEMPLATE_REGISTRY.get(tid, {})
        req = meta.get("required_slots", [])
        opt = meta.get("optional_slots", [])
        
        line = (
            f"- ID: {tid} (Confidence: {score:.2f})\n"
            f"  Required Params: {json.dumps(req)}\n"
            f"  Optional Params: {json.dumps(opt)}"
        )
        lines.append(line)
        
    return "\n\n".join(lines)

def extract_first_json_object(s: str):
    start = s.find("{")
    while start != -1:
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(s)):
            ch = s[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            else:
                if ch == '"':
                    in_str = True
                    continue
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        return s[start:i+1]
        start = s.find("{", start + 1)
    return None

def force_json(txt: str):
    s = FENCE.sub("", txt or "").strip()
    
    try:
        return json.loads(s)
    except Exception:
        cand = extract_first_json_object(s)
        if cand:
            return json.loads(cand)
    
    raise ValueError("LLM did not return valid JSON")

def parse_params_datetime(params: dict):
    out = {}
    for k, v in (params or {}).items():
        if isinstance(v, str) and k.endswith("_ts"):
            out[k] = dtparser.parse(v)
        else:
            out[k] = v
    return out

def call_vertex(prompt_user: str):
    max_tokens  = int(os.getenv("B311_LLM_MAX_TOKENS", "2048"))
    temperature = float(os.getenv("B311_LLM_TEMPERATURE", "0.1"))
    log_llm_raw("input prompt", PROMPT_SYSTEM + "\n\n" + prompt_user)

    cfg = GenerationConfig(
        temperature=temperature,
        max_output_tokens=max_tokens,
    )
    resp = model.generate_content(
        contents=[prompt_user],
        generation_config=cfg,
    )
    
    log_llm_raw("response received", resp.text.strip())
    return (resp.text or "").strip()

def call_llm_router(question: str, candidates: List[Tuple[str, float]], schema_snippet: str, rules_snippet: str):
    logger.info("Router for question=%r", question)
    logger.info("Candidates: %s", ", ".join([f"{t}({s:.2f})" for t, s in candidates]))

    user_prompt = f"""User question:
                    {question}

                    Schema:
                    {schema_snippet[:1500]}

                    Rules:
                    {rules_snippet[:1000]}

                    Candidate templates (IDs):
                    {pretty_candidates(candidates)}

                    Return JSON only. Do not change the parameter names. No code blocks, no backticks, no extra words."""
    
    txt = call_vertex(user_prompt + "\n\nRespond with a single minified JSON object only.")
    try:
        logger.infor("Attempting to parse LLM output as JSON")
        data = force_json(txt)
        logger.info("LLM produced valid JSON on first attempt")
        
    except Exception:
        txt2 = call_vertex(user_prompt + "\n\nRespond with a single minified JSON object only.")
        try:
            logger.info("Retrying JSON parse on second LLM output")
            data = force_json(txt2)
            logger.info("LLM produced valid JSON on second attempt")
            
        except Exception:
            logger.warning("LLM failed to produce valid JSON after retry")
            return {
                "template_id": None,
                "params": {},
                "confidence": 0.0,
                "needs_clarification": True,
                "clarify_question": "I couldn’t extract a valid template. Could you rephrase or add a time range/geography?"
            }

    if "params" in data:
        data["params"] = parse_params_datetime(data["params"])

    logger.info("LLM chose template_id=%s confidence=%s needs_clarification=%s",
                data.get("template_id"), data.get("confidence"), data.get("needs_clarification"))
    logger.debug("LLM params=%s", data.get("params"))
    return data

def validate_params(template_id: str, p: dict):
    
    def clamp(v, lo, hi, default):
        try: v = int(v)
        except Exception: return default
        return max(lo, min(v, hi))

    if template_id in ("TOPN_NEIGHBORHOODS", "GEO_POLYGON", "GEO_RADIUS", "TEXT_SEARCH"):
        p["k"] = clamp(p.get("k", 50), 1, 100, 50)
    if template_id == "GEO_RADIUS":
        p["radius_m"] = clamp(p.get("radius_m", 1000), 1, 5000, 1000)
        p["center_lon"] = float(p["center_lon"])
        p["center_lat"]  = float(p["center_lat"])
    if template_id == "BACKLOG":
        p["older_than_days"] = clamp(p.get("older_than_days", 30), 1, 180, 30)
    for key in ("start_ts","end_ts"):
        if key in p and not isinstance(p[key], datetime):
            raise ValueError(f"{key} must be ISO8601 datetime. Got: {p[key]}")

    logger.info("Validated params for %s: %s", template_id, p)
    return p

def fill_params(llm_out: dict):
    tid = llm_out.get("template_id")
    if not tid:
        return llm_out
    
    meta = TEMPLATE_REGISTRY.get(tid, {})
    req = meta.get("required_slots", [])
    opt = meta.get("optional_slots", [])
    
    params = llm_out.get("params", {})
    if params is None:
        params = {}
    all_slots = req + opt
    
    for slot in all_slots:
        if slot not in params:
            params[slot] = None 

    llm_out["params"] = params
    return llm_out
    
def choose_and_fill(question: str, schema_snippet: str = "", rules_snippet: str = ""):
    cands = top_k(question, k=3)
    
    llm_out = call_llm_router(question, cands, schema_snippet, rules_snippet)
    if llm_out.get("needs_clarification", False):
        return llm_out
    tid = llm_out.get("template_id")
    
    try:
        params = validate_params(tid, llm_out.get("params", {}) or {})
    except Exception as e:
        logger.exception("Param validation failed for template=%s", tid)
        return {
            "template_id": None,
            "params": {},
            "confidence": llm_out.get("confidence", 0.0),
            "needs_clarification": True,
            "clarify_question": f"Parameters invalid: {e}. Could you clarify?"
        }
    
    llm_out["params"] = params
    print(llm_out)
    llm_out = fill_params(llm_out)
    print(llm_out)
    return llm_out
