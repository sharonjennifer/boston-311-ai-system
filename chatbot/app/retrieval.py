# app/retrieval.py
from dataclasses import dataclass
from typing import List, Tuple
import json, os, time, numpy as np, faiss
from sentence_transformers import SentenceTransformer
import logging

logger = logging.getLogger("b311.retrieval")

CORPUS_DIR = os.path.join(os.path.dirname(__file__), "..", "corpus")
EMBED_PATH = os.path.join(CORPUS_DIR, "tmpl.faiss")
META_PATH  = os.path.join(CORPUS_DIR, "tmpl_meta.json")

_model = None
def _model_load():
    global _model
    if _model is None:
        t0 = time.time()
        logger.info("Loading embedding model all-MiniLM-L6-v2 ...")
        _model = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Model loaded in %.2fs", time.time() - t0)
    return _model

@dataclass
class Card:
    id: str; title: str; description: str; examples: list

def _load_cards() -> List[Card]:
    cards: List[Card] = []
    path = os.path.join(CORPUS_DIR, "templates.jsonl")
    logger.info("Loading template cards from %s", path)
    with open(path) as f:
        for line in f:
            j = json.loads(line)
            cards.append(Card(j["id"], j["title"], j["description"], j.get("examples", [])))
    logger.info("Loaded %d template cards", len(cards))
    return cards

def build_index() -> None:
    logger.info("Building FAISS index...")
    t0 = time.time()
    cards = _load_cards()
    texts = [f"{c.title}. {c.description}. Examples: {', '.join(c.examples)}" for c in cards]
    model = _model_load()
    X = model.encode(texts, normalize_embeddings=True).astype("float32")
    index = faiss.IndexFlatIP(X.shape[1])
    index.add(X)
    os.makedirs(CORPUS_DIR, exist_ok=True)
    faiss.write_index(index, EMBED_PATH)
    with open(META_PATH, "w") as f:
        json.dump([c.__dict__ for c in cards], f)
    logger.info("FAISS index built with %d vectors in %.2fs", len(cards), time.time() - t0)

def top_k(query: str, k: int = 3) -> List[Tuple[str, float]]:
    if not os.path.exists(EMBED_PATH) or not os.path.exists(META_PATH):
        logger.warning("Index or metadata missing; building index...")
        build_index()
    logger.info("Retrieval start for query=%r", query)
    index = faiss.read_index(EMBED_PATH)
    with open(META_PATH) as f:
        meta = json.load(f)
    model = _model_load()
    q = model.encode([query], normalize_embeddings=True).astype("float32")
    D, I = index.search(q, min(k, len(meta)))

    # Log candidates + cosine scores
    pairs = []
    for j, i in enumerate(I[0]):
        tid = meta[i]["id"]
        score = float(D[0][j])
        pairs.append((tid, score))
        logger.info("  cand: %-24s  cos=%.4f", tid, score)

    return pairs
