import os
import sys
import pickle
import logging
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from huggingface_hub import login
from sentence_transformers import SentenceTransformer

load_dotenv()

logging.basicConfig(
    level=os.getenv("B311_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("b311.vector_store")

HF_TOKEN = os.getenv("HF_TOKEN")
if HF_TOKEN:
    try:
        login(token=HF_TOKEN)
        logger.info("Authenticated to Hugging Face with HF_TOKEN.")
    except Exception as e:
        logger.warning(f"Failed to login to Hugging Face: {e}")
else:
    logger.warning("HF_TOKEN not set; using anonymous Hugging Face access (may be rate-limited).")

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BASE_DIR = os.path.dirname(CURRENT_FILE_DIR)
DATA_DIR_ENV = os.getenv("B311_DATA_DIR", "data")

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
sys.path.append(str(PROJECT_ROOT))

from rag.data_definitions import VALID_VALUES  # noqa: E402

if os.path.isabs(DATA_DIR_ENV):
    DATA_DIR = DATA_DIR_ENV
else:
    DATA_DIR = os.path.join(DEFAULT_BASE_DIR, DATA_DIR_ENV)

META_FILENAME = os.getenv("B311_META_FILENAME", "attributes.pkl")
META_PATH = os.path.join(DATA_DIR, META_FILENAME)

logger.info(f"Vector Store configured using DATA_DIR: {DATA_DIR}")



class AttributeRetriever:
    
    def __init__(self, model_name=None, force_rebuild=False):
        self.model_name = model_name or os.getenv("B311_EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        self.embeddings = {}
        self.lookups = {}

        artifacts_exist = os.path.exists(META_PATH)

        if not force_rebuild and artifacts_exist:
            logger.info("Artifacts found. Loading embeddings from disk...")
            self.load()
        else:
            if force_rebuild:
                logger.info("Force rebuild requested.")
            else:
                logger.info("Artifacts not found. Building new embeddings store...")

            self.model = SentenceTransformer(self.model_name)
            self.build_indices()

    def build_indices(self):
        logger.info(f"Building embedding matrices in {DATA_DIR}...")
        os.makedirs(DATA_DIR, exist_ok=True)

        for col, data in VALID_VALUES.items():
            if isinstance(data, dict):
                texts = list(data.keys())
                values = list(data.values())
            else:
                texts = data
                values = data

            logger.debug(f"Embedding {len(texts)} items for column '{col}'...")
            embs = self.model.encode(
                texts,
                convert_to_numpy=True,
                normalize_embeddings=True,
            ).astype("float32")

            self.embeddings[col] = embs
            self.lookups[col] = values
            logger.info(f"-> Built embeddings for '{col}' with {len(values)} items.")

        try:
            with open(META_PATH, "wb") as f:
                pickle.dump(
                    {
                        "embeddings": self.embeddings,
                        "lookups": self.lookups,
                        "model_name": self.model_name,
                    },
                    f,
                )
            logger.info(f"Successfully saved embeddings metadata to {META_PATH}")
        except Exception as e:
            logger.error(f"Failed to save artifacts: {e}")

    def load(self):
        try:
            with open(META_PATH, "rb") as f:
                data = pickle.load(f)

            self.embeddings = data["embeddings"]
            self.lookups = data["lookups"]
            self.model_name = os.getenv("B311_EMBEDDING_MODEL", data.get("model_name", self.model_name))

            logger.info("Loading embedding model for query encoding...")
            self.model = SentenceTransformer(self.model_name)
            logger.info("Successfully loaded RAG system from disk.")
        except Exception as e:
            logger.error(f"Failed to load artifacts: {e}. Triggering fallback rebuild.")
            self.model = SentenceTransformer(self.model_name)
            self.build_indices()

    def search(self, column, query, k=1, threshold=0.0):
        if column not in self.embeddings:
            logger.warning(f"Attempted search on non-existent column: {column}")
            return []

        mat = self.embeddings[column]
        values = self.lookups[column]

        q = self.model.encode(
            [query],
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype("float32")[0]

        scores = mat @ q

        n = scores.shape[0]
        k = min(k, n)
        if k <= 0:
            return []

        top_idx = np.argpartition(-scores, k - 1)[:k]
        top_idx = top_idx[np.argsort(-scores[top_idx])]

        results = []
        for idx in top_idx:
            score = float(scores[idx])
            if score >= threshold:
                results.append(
                    {
                        "value": values[idx],
                        "score": round(score, 4),
                    }
                )
        return results


retriever_instance = None


def get_retriever(force_rebuild=False):
    global retriever_instance
    if retriever_instance is None or force_rebuild:
        logger.info("Initializing AttributeRetriever singleton (force_rebuild=%s)", force_rebuild)
        retriever_instance = AttributeRetriever(force_rebuild=force_rebuild)
    return retriever_instance


def retrieve_keywords(parsed_entities, k=1, threshold=0.1):
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
        "reason": "Noise Disturbance",
    }

    for col, query in test_queries.items():
        results = retriever.search(col, query, k=3, threshold=0.1)
        print(f"Results for column '{col}' and query '{query}':")
        for res in results:
            print(f"  - Value: {res['value']}, Score: {res['score']}")
        print()