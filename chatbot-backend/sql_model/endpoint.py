import json
import logging
import os

from dotenv import load_dotenv
from google.cloud import aiplatform_v1
from google.api_core.client_options import ClientOptions
from google.api import httpbody_pb2

from sql_model.prompt_template import build_prompt

load_dotenv()

PROJECT_ID = os.getenv("B311_PROJECT_ID")
REGION = os.getenv("B311_VERTEX_LOCATION", "us-central1")
LOG_LEVEL = os.getenv("B311_LOG_LEVEL", "INFO").upper()
ENDPOINT_ID = os.getenv("B311_SQLCODER_ENDPOINT_ID")

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("b311.sql_endpoint")

if not PROJECT_ID or not REGION or not ENDPOINT_ID:
    logger.warning(
        "PROJECT_ID, REGION, or B311_SQLCODER_ENDPOINT_ID is missing. "
        "Check your .env configuration."
    )

client = aiplatform_v1.PredictionServiceClient(
    client_options=ClientOptions(api_endpoint=f"{REGION}-aiplatform.googleapis.com")
)

endpoint_path = client.endpoint_path(
    project=PROJECT_ID,
    location=REGION,
    endpoint=ENDPOINT_ID,
)

logger.info("Using SQLCoder endpoint: %s", endpoint_path)

def generate_sql(question, keywords):
    prompt = build_prompt(question, keywords)
    logger.debug("SQL prompt sent to endpoint:\n%s", prompt)

    request_body = {
        "model": "openapi",
        "prompt": prompt,
        "max_tokens": 256,
        "temperature": 0.1,
    }

    http_body = httpbody_pb2.HttpBody(
        content_type="application/json",
        data=json.dumps(request_body).encode("utf-8"),
    )

    try:
        logger.info("Sending request to Vertex AI SQLCoder endpoint…")
        response = client.raw_predict(
            endpoint=endpoint_path,
            http_body=http_body,
        )

        resp_json = json.loads(response.data.decode("utf-8"))
        logger.debug("Raw response JSON from endpoint: %s", resp_json)

        sql = resp_json["choices"][0].get("text", "").strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()

        logger.info("Generated SQL: %s", sql)
        return sql

    except (KeyError, IndexError) as e:
        logger.error("Error extracting SQL from response JSON: %s", e)
        return "SELECT 'Error extracting SQL from model response' AS error_message;"
    except Exception as e:
        logger.error("Vertex SQL generation error: %s", e)
        return "SELECT 'SQL generation failed' AS error_message;"