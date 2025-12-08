import os
import json
import logging
import vertexai

from pathlib import Path
from dotenv import load_dotenv
from vertexai.generative_models import GenerativeModel, GenerationConfig

load_dotenv()
logging.basicConfig(
    level=os.getenv("B311_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("b311.parser")

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent.parent
SECRET_KEY_PATH = PROJECT_ROOT / "secrets" / "sabari_secret.json"

if SECRET_KEY_PATH.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SECRET_KEY_PATH)
    logger.info(f"Authentication Configured: Using key at {SECRET_KEY_PATH}")
else:
    logger.warning(f"Key not found at {SECRET_KEY_PATH}. Attempting to use default credentials...")

PROJECT_ID = os.getenv("B311_PROJECT_ID")
LOCATION = os.getenv("B311_VERTEX_LOCATION", "us-central1")
MODEL_ID = os.getenv("B311_EXTRACTOR_MODEL", "gemini-2.0-flash-001")

# System Prompt
PARSER_INSTRUCTION = """
You are an expert entity extractor for the Boston 311 database.
Your goal is to extract search terms from the user's natural language query into a strict JSON format.

Categories to extract:
- neighborhood: Locations, districts, or areas (e.g., "Southie", "Dorchester").
- subject: City departments or agencies (e.g., "Boston police", "Animal Control").
- case_status: "Open" or "Closed" only.
- source: How the request was made (e.g., "App", "Phone", "Citizen Connect").
- reason: Broad categories (e.g., "Sanitation", "Street Lights", "Trees").
- type: Specific complaint types (e.g., "Missed Trash", "Pothole", "Needle Pickup").
- on_time: Whether the cases were resolved "ONTIME" or "OVERDUE".

Rules:
1. If a category is not explicitly mentioned, return null.
2. Do NOT try to guess valid database values. Extract the exact words the user used (e.g., extract "rats" not "Rodent Activity").
3. Output valid JSON only.
"""



class QueryParser:
    def __init__(self):
        try:
            logger.info(f"Initializing Vertex AI (Project: {PROJECT_ID}, Loc: {LOCATION})")
            vertexai.init(project=PROJECT_ID, location=LOCATION)
            self.model = GenerativeModel(
                MODEL_ID,
                system_instruction=[PARSER_INSTRUCTION]
            )
            logger.info(f"Extractor model loaded: {MODEL_ID}")
        except Exception as e:
            logger.critical(f"Failed to initialize Vertex AI: {e}")
            raise

    def parse(self, user_text: str, context: str = "") -> dict:
        """
        Parse user query with optional conversation context
        
        Args:
            user_text: The current user question
            context: Optional conversation history for context
        """
        logger.info(f"Parsing query: '{user_text}'")
        
        # Build prompt with context if available
        full_prompt = user_text
        if context:
            full_prompt = f"{context}\n\nCurrent question: {user_text}\n\nExtract entities from the CURRENT question, using context to fill in missing details."
            logger.debug(f"Using conversation context")
        
        try:
            response = self.model.generate_content(
                full_prompt,
                generation_config=GenerationConfig(
                    response_mime_type="application/json",
                    temperature=0.0
                )
            )

            raw_text = response.text
            logger.debug(f"Raw parser response text: {raw_text}")

            extracted_data = json.loads(raw_text)

            if isinstance(extracted_data, list):
                if len(extracted_data) == 0:
                    logger.error("Model returned an empty JSON list.")
                    return {}
                first = extracted_data[0]
                if not isinstance(first, dict):
                    logger.error(f"Unexpected JSON structure (list but first element is {type(first)}).")
                    return {}
                extracted_data = first
            elif not isinstance(extracted_data, dict):
                logger.error(f"Unexpected JSON structure: {type(extracted_data)}")
                return {}
            clean_data = {k: v for k, v in extracted_data.items() if v is not None}

            logger.info(f"Extracted entities: {clean_data}")
            return clean_data

        except json.JSONDecodeError:
            logger.error("Model did not return valid JSON.")
            return {}
        except Exception as e:
            logger.error(f"Error during parsing: {e}")
            return {}

def parse_query(user_text: str, context: str = ""):
    """Parse query with optional conversation context"""
    parser = get_parser()
    return parser.parse(user_text, context)

parser_instance = None

def get_parser():
    global parser_instance
    if parser_instance is None:
        parser_instance = QueryParser()
    return parser_instance


def parse_query(user_text: str, context: str = ""):
    """Parse query with optional conversation context"""
    parser = get_parser()
    return parser.parse(user_text, context)