# app/router_llm.py
import json
import re
from typing import Dict, Any, List, Tuple
from datetime import datetime
from dateutil import parser as dtparser

from app.retrieval import top_k
from app.config import LLM_API_BASE, LLM_API_KEY, LLM_MODEL
from openai import OpenAI

_client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_API_BASE)

PROMPT_SYSTEM = """You are a Boston 311 SQL router. You MUST select exactly one template from the provided candidates
and fill its parameters. Only use the schema and rules given. Output STRICT JSON with keys:
{"template_id": "...", "params": {...}, "confidence": 0..1, "needs_clarification": false, "clarify_question": ""}

Constraints:
- Use ISO8601 timestamps (e.g., 2025-11-10T08:00:00-05:00) for any datetime parameters (assume America/New_York if user gives relative time).
- If a required parameter is missing or ambiguous, set needs_clarification=true and ask ONE short clarifying question.
- Do not invent parameters outside the chosen template.
- Do not expand user time ranges beyond what was requested. Minimal defaults are OK if user didn't specify.
- Output JSON ONLY. No prose, no code blocks, no extra text.
"""

def _pretty_candidates(cands: List[Tuple[str, float]]) -> str:
    return "\n".join([f"- {tid} (score≈{score:.2f})" for tid, score in cands])

def _parse_params_datetime(params: dict) -> dict:
    out = {}
    for k, v in params.items():
        if k.endswith("_ts") and isinstance(v, str):
            out[k] = dtparser.parse(v)
        else:
            out[k] = v
    return out

# app/router_llm.py  (replace the fence/JSON extraction helpers)

import json
import re
# ... keep other imports ...

_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

def _extract_first_json_object(s):
    """
    Scan the string and return the first balanced {...} JSON object (as text),
    or None if not found.
    """
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
        # no balance from this start; find next '{'
        start = s.find("{", start + 1)
    return None

def _force_json(txt: str) -> dict:
    # 1) strip code fences/backticks if present
    s = _FENCE.sub("", txt).strip()
    # 2) try direct parse first
    try:
        return json.loads(s)
    except Exception:
        pass
    # 3) try to locate first balanced JSON object
    candidate = _extract_first_json_object(s)
    if candidate:
        return json.loads(candidate)
    # 4) give up
    raise ValueError("LLM did not return valid JSON")

def _call_openai(messages: list, temperature=0.1, max_tokens=400, force_json=True) -> str:
    # response_format is ignored by some servers (Ollama), but safe to include
    kwargs = dict(
        model=LLM_MODEL,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    if force_json:
        kwargs["response_format"] = {"type": "json_object"}
    resp = _client.chat.completions.create(**kwargs)
    return resp.choices[0].message.content or ""

def call_llm_router(question: str, candidates: List[Tuple[str, float]], schema_snippet: str, rules_snippet: str) -> dict:
    user_prompt = f"""User question:
{question}

Schema:
{schema_snippet[:1500]}

Rules:
{rules_snippet[:1000]}

Candidate templates (IDs):
{_pretty_candidates(candidates)}

Return JSON only. No code blocks, no backticks, no extra words."""
    # First attempt
    txt = _call_openai(
        [
            {"role": "system", "content": PROMPT_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
        force_json=True,
    )
    try:
        data = _force_json(txt)
    except Exception:
        # Second attempt: ultra-strict reminder
        txt2 = _call_openai(
            [
                {"role": "system", "content": PROMPT_SYSTEM},
                {"role": "user", "content": user_prompt + "\n\nRespond with a single minified JSON object only."},
            ],
            temperature=0.0,
            force_json=True,
            max_tokens=300,
        )
        try:
            data = _force_json(txt2)
        except Exception:
            # As a safe fallback, ask for clarification instead of 500
            return {
                "template_id": None,
                "params": {},
                "confidence": 0.0,
                "needs_clarification": True,
                "clarify_question": "I couldn’t extract a valid template. Could you rephrase or add a time range/geography?"
            }

    # Post-process timestamps
    if "params" in data:
        data["params"] = _parse_params_datetime(data["params"])
    return data

def _validate_params(template_id: str, p: dict) -> dict:
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
    return p

def choose_and_fill(question: str, schema_snippet: str = "", rules_snippet: str = "") -> Dict[str, Any]:
    cands = top_k(question, k=5)
    llm_out = call_llm_router(question, cands, schema_snippet, rules_snippet)
    if llm_out.get("needs_clarification", False):
        return llm_out
    tid = llm_out.get("template_id")
    params = _validate_params(tid, llm_out.get("params", {}) or {})
    llm_out["params"] = params
    return llm_out
