import os
import logging
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
from enum import Enum

logger = logging.getLogger("b311.routers")

PROJECT_ID = os.getenv("B311_PROJECT_ID")
LOCATION = os.getenv("B311_VERTEX_LOCATION", "us-central1")
MODEL_ID = os.getenv("B311_ROUTER_MODEL", "gemini-2.0-flash-exp")

vertexai.init(project=PROJECT_ID, location=LOCATION)
router_model = GenerativeModel(MODEL_ID)

class RouteDecision(Enum):
    RELEVANT = "relevant"
    OFF_TOPIC = "off_topic"
    DATA_QUERY = "data_query"
    PROCEDURAL = "procedural"


# ROUTER 1: BOSTON 311 TOPIC RELEVANCE CLASSIFIER

TOPIC_CLASSIFIER_PROMPT = """
You are a topic classifier for a Boston 311 service request chatbot.

Your ONLY job is to determine if a question is about Boston 311 services or completely off-topic.

RELEVANT topics (respond with "YES"):
- Questions about Boston 311 service requests (potholes, trash, graffiti, streetlights, etc.)
- Questions about Boston city services, departments, neighborhoods
- Questions about reporting issues, case status, resolution times
- Questions about how to use 311, who to contact for city services
- Any question that could reasonably relate to Boston city government services

OFF-TOPIC questions (respond with "NO"):
- Literature, books, Shakespeare, poetry
- General knowledge questions unrelated to Boston
- Math problems, jokes, recipes, sports
- Personal advice unrelated to city services
- Questions about other cities (unless comparing to Boston)

Respond with ONLY "YES" or "NO" - nothing else.

Examples:
Q: "How many potholes were reported last week?" → YES
Q: "Who handles graffiti removal?" → YES
Q: "What's the 3rd book of Shakespeare?" → NO
Q: "Tell me a joke" → NO
Q: "How do I report a streetlight issue?" → YES
Q: "What's the capital of France?" → NO
"""


def check_topic_relevance(question):
    logger.info(f"[Router 1] Checking topic relevance for: '{question}'")
    
    try:
        response = router_model.generate_content(
            f"{TOPIC_CLASSIFIER_PROMPT}\n\nQuestion: {question}\n\nAnswer (YES or NO):",
            generation_config=GenerationConfig(
                temperature=0.0,
                max_output_tokens=10
            )
        )
        
        decision = response.text.strip().upper()
        is_relevant = "YES" in decision
        
        logger.info(f"[Router 1] Decision: {decision} → {'RELEVANT' if is_relevant else 'OFF-TOPIC'}")
        return is_relevant
        
    except Exception as e:
        logger.error(f"[Router 1] Classification failed: {e}. Defaulting to RELEVANT.")
        return True



# ROUTER 2: QUERY TYPE CLASSIFICATION

QUERY_TYPE_CLASSIFIER_PROMPT = """
You are a query type classifier for a Boston 311 chatbot.

Classify the question into ONE of these types:

DATA_QUERY - Questions that need to query the database:
- "How many..." (counts, statistics)
- "Show me..." (lists, specific cases)
- "What are the most..." (rankings, comparisons)
- "Compare..." (comparisons)
- "When was..." (temporal queries)
- Any question requiring data analysis

PROCEDURAL - Questions about processes, contacts, or how-to:
- "Who handles..." (responsible departments)
- "How do I report..." (reporting procedures)
- "What's the process for..." (procedures)
- "Who should I contact..." (contact information)
- "What department is responsible..." (organizational questions)

Respond with ONLY "DATA_QUERY" or "PROCEDURAL" - nothing else.

Examples:
Q: "How many potholes in Dorchester?" → DATA_QUERY
Q: "Who handles graffiti removal?" → PROCEDURAL
Q: "Show me open cases in South End" → DATA_QUERY
Q: "How do I report a streetlight issue?" → PROCEDURAL
Q: "What's the average resolution time?" → DATA_QUERY
Q: "Which department handles snow removal?" → PROCEDURAL
"""


def classify_query_type(question):
    logger.info(f"[Router 2] Classifying query type for: '{question}'")
    
    try:
        response = router_model.generate_content(
            f"{QUERY_TYPE_CLASSIFIER_PROMPT}\n\nQuestion: {question}\n\nClassification:",
            generation_config=GenerationConfig(
                temperature=0.0,
                max_output_tokens=20
            )
        )
        
        classification = response.text.strip().upper()
        
        if "PROCEDURAL" in classification:
            query_type = "procedural"
        else:
            query_type = "data_query"
        
        logger.info(f"[Router 2] Classification: {query_type.upper()}")
        return query_type
        
    except Exception as e:
        logger.error(f"[Router 2] Classification failed: {e}. Defaulting to data_query.")
        return "data_query"



# FAQ / PROCEDURAL RESPONSE HANDLER

FAQ_KNOWLEDGE = """
BOSTON 311 PROCEDURAL KNOWLEDGE:

DEPARTMENTS & RESPONSIBILITIES:
- Public Works Department (PWD): Potholes, street repairs, sidewalks, street cleaning
- Parks Department: Park maintenance, trees, playgrounds
- Transportation Department: Traffic signals, parking enforcement, bike lanes
- ISD (Inspectional Services): Building code violations, housing complaints
- Animal Control: Stray animals, wildlife issues
- Sanitation: Trash collection, recycling, bulk item pickup

HOW TO REPORT ISSUES:
- Call: 311 (or 617-635-4500)
- Online: boston.gov/311
- Mobile App: BOS:311 app (iOS/Android)
- In person: City Hall

COMMON CONTACTS:
- General 311: Call 311 or visit boston.gov/311
- Graffiti removal: Public Works Department via 311
- Pothole repair: Public Works Department via 311
- Missed trash/recycling: Call 311 with your address
- Street light issues: Public Works Department via 311
- Tree issues: Parks Department via 311
- Parking enforcement: Transportation Department via 311

RESPONSE TIMES:
- Emergency issues: Typically within 24 hours
- Non-emergency: Varies by issue type and department
- You can track your case status online with your case number
"""

PROCEDURAL_SYSTEM_PROMPT = f"""
You are a helpful Boston 311 assistant answering procedural questions.

Use this knowledge to answer questions about processes, contacts, and how-to:

{FAQ_KNOWLEDGE}

Guidelines:
1. Be helpful and direct
2. Provide specific department names when known
3. Include multiple contact methods when relevant
4. Keep answers concise (2-3 sentences)
5. If you're not sure, suggest they call 311 for the most accurate information

Answer in a friendly, professional tone like a city employee helping a resident.
"""


def handle_procedural_question(question):
    logger.info(f"[FAQ Handler] Processing procedural question: '{question}'")
    
    try:
        response = router_model.generate_content(
            [PROCEDURAL_SYSTEM_PROMPT, f"Question: {question}\n\nAnswer:"],
            generation_config=GenerationConfig(
                temperature=0.3,
                max_output_tokens=300
            )
        )
        
        answer = response.text.strip()
        logger.info(f"[FAQ Handler] Generated procedural answer")
        return answer
        
    except Exception as e:
        logger.error(f"[FAQ Handler] Failed to generate answer: {e}")
        return (
            "For information about Boston 311 services and how to report issues, "
            "please call 311 or visit boston.gov/311. "
            "You can also download the BOS:311 mobile app."
        )



# MAIN ROUTING FUNCTION

def route_question(question: str) -> tuple[str, dict]:
    is_relevant = check_topic_relevance(question)
    
    if not is_relevant:
        logger.info(f"[Routing] Question is OFF-TOPIC")
        return "off_topic", {
            "message": (
                "I'm specifically designed to help with Boston 311 service requests. "
                "I can answer questions about potholes, trash collection, graffiti, "
                "streetlights, and other city services in Boston.\n\n"
                "For other topics, I'd recommend using a general-purpose assistant."
            )
        }
    
    query_type = classify_query_type(question)
    
    logger.info(f"[Routing] Final route: {query_type.upper()}")
    return query_type, {}