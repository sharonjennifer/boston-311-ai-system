# app/retrieval.py
from dataclasses import dataclass
from typing import List, Tuple
import json, os, numpy as np, faiss
from sentence_transformers import SentenceTransformer

CORPUS_DIR = os.path.join(os.path.dirname(__file__), "..", "corpus")
EMBED_PATH = os.path.join(CORPUS_DIR, "tmpl.faiss")
META_PATH  = os.path.join(CORPUS_DIR, "tmpl_meta.json")

_model = None
def _model_load():
    global _model
    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model

@dataclass
class Card:
    id: str; title: str; description: str; examples: list

def _load_cards() -> List[Card]:
    cards: List[Card] = []
    with open(os.path.join(CORPUS_DIR, "templates.jsonl")) as f:
        for line in f:
            j = json.loads(line)
            cards.append(Card(j["id"], j["title"], j["description"], j.get("examples", [])))
    return cards

def build_index() -> None:
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

def top_k(query: str, k: int = 3) -> List[Tuple[str, float]]:
    if not os.path.exists(EMBED_PATH) or not os.path.exists(META_PATH):
        build_index()
    index = faiss.read_index(EMBED_PATH)
    with open(META_PATH) as f:
        meta = json.load(f)
    model = _model_load()
    q = model.encode([query], normalize_embeddings=True).astype("float32")
    D, I = index.search(q, min(k, len(meta)))
    return [(meta[i]["id"], float(D[0][j])) for j, i in enumerate(I[0])]
