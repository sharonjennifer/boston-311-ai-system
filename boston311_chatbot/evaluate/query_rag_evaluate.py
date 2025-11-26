import os
import sys
import csv
import json
import time
import logging

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=os.getenv("B311_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("b311.system_test")

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
sys.path.append(str(PROJECT_ROOT))

from rag.vector_store import AttributeRetriever
from query_parser.query_parser import QueryParser

TEST_FILE = PROJECT_ROOT / "test_files" / "query_rag_test.csv"



def normalize_json(data):
    return json.dumps(data, sort_keys=True)

def run_end_to_end_test():
    try:
        logger.info("Initializing Gemini Parser...")
        parser = QueryParser()
        
        logger.info("Initializing RAG Vector Store...")
        vdb = AttributeRetriever(force_rebuild=False)
    except Exception as e:
        logger.critical(f"Initialization failed: {e}")
        return
    
    if not os.path.exists(TEST_FILE):
        logger.error(f"Test file not found: {TEST_FILE}")
        return

    test_cases = []
    with open(TEST_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            try:
                expected_dict = json.loads(row['expected_json'])
                test_cases.append((row['query'], expected_dict))
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid row in CSV: {row} - {e}")

    logger.info(f"Loaded {len(test_cases)} system tests.")
    logger.info("-" * 80)
    
    passes = 0
    KEY_MAPPING = {
        "neighborhood": "neighborhood",
        "department": "department",
        "source": "source",
        "case_status": "case_status",
        "on_time": "on_time",
        "reason": "reason",
        "type": "type"
    }

    for query, expected_grounded_data in test_cases:
        print(f"\nQ: '{query}'")
        
        extracted_raw = parser.parse(query)
        final_grounded_data = {}
        
        for key, raw_val in extracted_raw.items():
            if key in KEY_MAPPING:
                vdb_col = KEY_MAPPING[key]
                
                results = vdb.search(vdb_col, raw_val, k=1, threshold=0.4)
                
                if results:
                    best_match = results[0]['value']
                    final_grounded_data[key] = best_match
                    print(f"└─ Grounded '{raw_val}' -> '{best_match}' (Score: {results[0]['score']})")
                else:
                    print(f"└─ No RAG match for '{raw_val}' in '{vdb_col}'")
                    final_grounded_data[key] = raw_val
            else:
                final_grounded_data[key] = raw_val

        is_pass = True
        for k, v in expected_grounded_data.items():
            if final_grounded_data.get(k) != v:
                is_pass = False
                print(f"Mismatch on '{k}': Expected '{v}', Got '{final_grounded_data.get(k)}'")
        
        if is_pass:
            passes += 1
            print("PASS")
        else:
            print("FAIL")
            
        time.sleep(0.5) 

    logger.info("-" * 80)
    accuracy = (passes / len(test_cases)) * 100
    logger.info(f"System Test Complete. Accuracy: {accuracy:.2f}% ({passes}/{len(test_cases)})")

if __name__ == "__main__":
    run_end_to_end_test()