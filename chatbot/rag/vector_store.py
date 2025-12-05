import faiss
import pickle
import os
import sys
import logging
import numpy as np

from pathlib import Path
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv()
logging.basicConfig(
    level=os.getenv("B311_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("b311.vector_store")

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BASE_DIR = os.path.dirname(CURRENT_FILE_DIR)
DATA_DIR_ENV = os.getenv("B311_DATA_DIR", "data")

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
sys.path.append(str(PROJECT_ROOT))

from rag.data_definitions import VALID_VALUES

if os.path.isabs(DATA_DIR_ENV):
    DATA_DIR = DATA_DIR_ENV
else:
    DATA_DIR = os.path.join(DEFAULT_BASE_DIR, DATA_DIR_ENV)

INDEX_FILENAME = os.getenv("B311_INDEX_FILENAME", "attributes.faiss")
META_FILENAME = os.getenv("B311_META_FILENAME", "attributes.pkl")
INDEX_PATH = os.path.join(DATA_DIR, INDEX_FILENAME)
META_PATH = os.path.join(DATA_DIR, META_FILENAME)

logger.info(f"Vector Store configured using DATA_DIR: {DATA_DIR}")



class AttributeRetriever:
    def __init__(self, model_name=None, force_rebuild=False):
        self.model_name = model_name or os.getenv("B311_EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        self.indices = {}
        self.lookups = {} 
        
        artifacts_exist = os.path.exists(INDEX_PATH) and os.path.exists(META_PATH)
        
        if not force_rebuild and artifacts_exist:
            logger.info("Artifacts found. Loading existing index from disk...")
            self.load()
        else:
            if force_rebuild:
                logger.info("Force rebuild requested.")
            else:
                logger.info("Artifacts not found. Building new index...")
            
            self.model = SentenceTransformer(self.model_name)
            self.build_indices()

    def build_indices(self):
        logger.info(f"Building indices in {DATA_DIR}...")
        os.makedirs(DATA_DIR, exist_ok=True)
        
        for col, data in VALID_VALUES.items():
            if isinstance(data, dict):
                texts = list(data.keys())
                values = list(data.values())
            else:
                texts = data
                values = data

            logger.debug(f"Embedding {len(texts)} items for column '{col}'...")
            embeddings = self.model.encode(texts)
            embeddings = np.array(embeddings).astype("float32")
            
            faiss.normalize_L2(embeddings)
            
            index = faiss.IndexFlatIP(embeddings.shape[1])
            index.add(embeddings)
            
            self.indices[col] = index
            self.lookups[col] = values
            logger.info(f"-> Built index for '{col}' with {len(values)} items.")

        try:
            with open(META_PATH, "wb") as f:
                pickle.dump(self.lookups, f)
            
            with open(INDEX_PATH, "wb") as f:
                pickle.dump(self.indices, f)
                
            logger.info(f"Successfully saved index and metadata to {DATA_DIR}")
        except Exception as e:
            logger.error(f"Failed to save artifacts: {e}")
            
    def load(self):
        try:
            with open(META_PATH, "rb") as f:
                self.lookups = pickle.load(f)
            
            with open(INDEX_PATH, "rb") as f:
                self.indices = pickle.load(f)
            
            logger.info("Loading embedding model for query encoding...")
            self.model = SentenceTransformer(self.model_name)
            logger.info("Successfully loaded RAG system from disk.")
            
        except Exception as e:
            logger.error(f"Failed to load artifacts: {e}. Triggering fallback rebuild.")
            self.model = SentenceTransformer(self.model_name)
            self.build_indices()

    def search(self, column, query, k=1, threshold=0.0):
        if column not in self.indices:
            logger.warning(f"Attempted search on non-existent column: {column}")
            return []

        q_embed = self.model.encode([query])
        faiss.normalize_L2(q_embed)
        
        D, I = self.indices[column].search(q_embed, k)
        
        results = []
        for i, idx in enumerate(I[0]):
            score = float(D[0][i])
            if idx >= 0 and score >= threshold:
                val = self.lookups[column][idx]
                results.append({
                    "value": val,
                    "score": round(score, 4)
                })
        return results

retriever_instance = None

def get_retriever(force_rebuild=False):
    global retriever_instance
    if retriever_instance is None or force_rebuild:
        logger.info("Initializing AttributeRetriever singleton (force_rebuild=%s)", force_rebuild)
        retriever_instance = AttributeRetriever(force_rebuild=force_rebuild)
    return retriever_instance


def retrieve_keywords(parsed_entities, k = 1, threshold = 0.1):
    retriever = get_retriever()
    hints = []

    if not isinstance(parsed_entities, dict):
        logger.warning("retrieve_keywords expected dict, got %s", type(parsed_entities))
        return hints

    for col, raw_value in parsed_entities.items():
        if raw_value is None:
            continue
        query_text = str(raw_value)
        results = retriever.search(column=col, query=query_text, k=k, threshold=threshold)
        
        if not results:
            logger.info(f"No RAG match for column='{col}' and query='{query_text}'")
            continue
        best = results[0]["value"]

        if isinstance(best, (int, float)):
            hint = f"{col}={best}"
        else:
            hint = f"{col}='{best}'"

        hints.append(hint)
        logger.info(f"RAG hint for {col}: {hint} (score={results[0]['score']})")

    return hints



if __name__ == "__main__":
    retriever = AttributeRetriever(force_rebuild=True)
    test_queries = {
        "neighborhood": "Downtown Boston",
        "department": "Public Works",
        "source": "Citizen report via mobile app",
        "type": "Pothole",
        "subject": "Street Cleaning",
        "reason": "Noise Disturbance"
    }
    
    for col, query in test_queries.items():
        results = retriever.search(col, query, k=3, threshold=0.1)
        print(f"Results for column '{col}' and query '{query}':")
        for res in results:
            print(f"  - Value: {res['value']}, Score: {res['score']}")
        print()