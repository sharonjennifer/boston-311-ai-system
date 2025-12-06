import os
import json
import logging
import vertexai

from vertexai.generative_models import GenerativeModel
from app.config import settings

LOG_LEVEL = os.getenv("B311_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("b311.answer_endpoint")

PROJECT_ID = getattr(settings, "PROJECT_ID", os.getenv("B311_PROJECT_ID"))
LOCATION = os.getenv("B311_VERTEX_LOCATION", "us-central1")
MODEL_ID = os.getenv("B311_ANSWER_MODEL_ID", "gemini-2.0-flash-001")

vertexai.init(project=PROJECT_ID, location=LOCATION)
answer_model = GenerativeModel(MODEL_ID)


def generate_answer(question, sql_query, rows):
    if not rows:
        return "I ran the query, but there were no matching results for your question."

    preview_rows = rows[:5]
    system_instructions = (
        "You are a helpful Boston 311 analytics assistant. "
        "You receive a user's natural language question and the SQL query results "
        "from a BigQuery table. "
        "Your job is to explain the answer in clear, concise language for a non-technical resident. "
        "Use concrete numbers from the data, avoid mentioning SQL or technical details unless asked, "
        "and keep the answer to 2–3 sentences."
    )

    user_prompt = (
        f"User question:\n{question}\n\n"
        f"SQL query that was executed (for context):\n{sql_query}\n\n"
        f"Top result rows as JSON:\n{json.dumps(preview_rows, indent=2)}\n\n"
        "Now, answer the user's question in natural language. "
        "If multiple rows are present, summarize the key trend. "
        "If only one row is present (like an hour_of_day and count), "
        "directly state the answer using those values."
    )

    try:
        response = answer_model.generate_content(
            [system_instructions, user_prompt]
        )
        text = (response.text or "").strip()
        if text:
            return text

        logger.warning("Gemini returned empty text; falling back to simple formatting.")
    except Exception as e:
        logger.error(f"Gemini answer generation failed: {e}. Falling back to simple text.")

    first = preview_rows[0]
    parts = [f"{k}: {v}" for k, v in first.items()]
    return "Here are the results: " + ", ".join(parts)
