# rag_system/vector_store.py

import faiss
import pickle
import os
import logging
import numpy as np

from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from rag_system.data_definitions import VALID_VALUES

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
            logger.info("Artifacts found. Loading existing index...")
            self._load()
        else:
            logger.info(f"Artifacts not found or rebuild forced. Initializing model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            self._build_indices()

    def _build_indices(self):
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
            logger.info(f"Metadata saved to {META_PATH}")
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
            
    def _load(self):
        logger.info("Rebuilding in-memory indices for dictionary support...")
        self.model = SentenceTransformer(self.model_name)
        self._build_indices()

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
                logger.debug(f"Search '{query}' in '{column}' -> Found: {val} (Score: {score:.4f})")
                results.append({
                    "value": val,
                    "score": round(score, 4)
                })
        return results