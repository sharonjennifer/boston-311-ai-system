import logging
import os
import csv

from pathlib import Path
from dotenv import load_dotenv
from vector_store import AttributeRetriever

load_dotenv()
logging.basicConfig(
    level=os.getenv("B311_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("b311.evaluate")

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
TEST_FILE_PATH = PROJECT_ROOT / "tests" / "rag_test.csv"

def load_test_cases(filepath):
    cases = []
    if not os.path.exists(filepath):
        logger.error(f"Test file not found at: {filepath}")
        return []
        
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cases.append((row['query'], row['column'], row['expected']))
        logger.info(f"Loaded {len(cases)} test cases from {filepath}")
        return cases
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}")
        return []

def run_evaluation():
    test_cases = load_test_cases(TEST_FILE_PATH)
    if not test_cases:
        return

    logger.info(f"Starting Evaluation on {len(test_cases)} test cases...")
    
    try:
        vdb = AttributeRetriever()
    except Exception as e:
        logger.critical(f"Failed to initialize Vector Store: {e}")
        return

    hits = 0
    
    header = f"{'QUERY':<25} | {'COL':<12} | {'EXPECTED':<30} | {'TOP RESULT':<30} | {'SCORE':<5} | {'STATUS'}"
    logger.info("-" * 120)
    logger.info(header)
    logger.info("-" * 120)

    for query, col, expected in test_cases:
        results = vdb.search(col, query, k=1, threshold=0.0)
        
        top_result = results[0]['value'] if results else "NO MATCH"
        score = results[0]['score'] if results else 0.0
        
        is_hit = (top_result == expected)
        
        if is_hit:
            hits += 1
            status = "PASS"
            log_level = logging.INFO
        else:
            status = "FAIL"
            log_level = logging.WARNING
            
        display_expected = (expected[:27] + '..') if len(expected) > 29 else expected
        display_result = (top_result[:27] + '..') if len(top_result) > 29 else top_result
        
        log_msg = f"{query:<25} | {col:<12} | {display_expected:<30} | {display_result:<30} | {score:.2f}  | {status}"
        logger.log(log_level, log_msg)

    accuracy = (hits / len(test_cases)) * 100
    logger.info("-" * 120)
    logger.info(f"Evaluation Complete. Overall Accuracy: {accuracy:.2f}%")
    logger.info("-" * 120)

if __name__ == "__main__":
    run_evaluation()