"""
RAG-based FAQ Retrieval System for Procedural Questions
"""
import os
import json
import pickle
import logging
from pathlib import Path
import numpy as np
from sentence_transformers import SentenceTransformer

logger = logging.getLogger("b311.faq_rag")

# Paths
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
FAQ_FILE = PROJECT_ROOT / "faq" / "boston311_faq.json"
FAQ_INDEX_FILE = PROJECT_ROOT / "data" / "faq_index.pkl"

# Model
EMBEDDING_MODEL = os.getenv("B311_EMBEDDING_MODEL", "all-MiniLM-L6-v2")


class FAQRetriever:
    """RAG-based FAQ retrieval system"""
    
    def __init__(self, force_rebuild=False):
        self.model = SentenceTransformer(EMBEDDING_MODEL)
        self.faqs = []
        self.embeddings = None
        
        # Load FAQ data
        if not FAQ_FILE.exists():
            logger.error(f"FAQ file not found: {FAQ_FILE}")
            raise FileNotFoundError(f"FAQ file missing: {FAQ_FILE}")
        
        with open(FAQ_FILE, 'r') as f:
            self.faqs = json.load(f)
        
        logger.info(f"Loaded {len(self.faqs)} FAQs from {FAQ_FILE}")
        
        # Load or build embeddings
        if FAQ_INDEX_FILE.exists() and not force_rebuild:
            self.load_index()
        else:
            self.build_index()
    
    def build_index(self):
        """Build FAQ embeddings index"""
        logger.info("Building FAQ embeddings...")
        
        # Combine question + keywords for better matching
        texts = []
        for faq in self.faqs:
            combined = f"{faq['question']} {' '.join(faq.get('keywords', []))}"
            texts.append(combined)
        
        # Generate embeddings
        self.embeddings = self.model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True
        ).astype('float32')
        
        # Save index
        FAQ_INDEX_FILE.parent.mkdir(exist_ok=True)
        with open(FAQ_INDEX_FILE, 'wb') as f:
            pickle.dump({
                'embeddings': self.embeddings,
                'faqs': self.faqs
            }, f)
        
        logger.info(f"Built and saved FAQ index to {FAQ_INDEX_FILE}")
    
    def load_index(self):
        """Load pre-built FAQ index"""
        logger.info(f"Loading FAQ index from {FAQ_INDEX_FILE}")
        
        with open(FAQ_INDEX_FILE, 'rb') as f:
            data = pickle.load(f)
        
        self.embeddings = data['embeddings']
        logger.info("FAQ index loaded successfully")
    
    def search(self, question: str, k=3, threshold=0.3):
        """
        Search for relevant FAQs
        
        Args:
            question: User's question
            k: Number of results to return
            threshold: Minimum similarity score (0-1)
        
        Returns:
            List of matching FAQs with scores
        """
        # Encode question
        q_embedding = self.model.encode(
            [question],
            convert_to_numpy=True,
            normalize_embeddings=True
        ).astype('float32')[0]
        
        # Compute similarities
        scores = self.embeddings @ q_embedding
        
        # Get top k
        top_k = min(k, len(scores))
        top_idx = np.argpartition(-scores, top_k - 1)[:top_k]
        top_idx = top_idx[np.argsort(-scores[top_idx])]
        
        # Filter by threshold and format results
        results = []
        for idx in top_idx:
            score = float(scores[idx])
            if score >= threshold:
                faq = self.faqs[idx].copy()
                faq['score'] = round(score, 4)
                results.append(faq)
                logger.debug(f"FAQ match: {faq['id']} (score: {score:.4f})")
        
        logger.info(f"Found {len(results)} FAQ matches above threshold {threshold}")
        return results


# Global instance
faq_retriever_instance = None


def get_faq_retriever(force_rebuild=False):
    """Get or create FAQ retriever singleton"""
    global faq_retriever_instance
    if faq_retriever_instance is None or force_rebuild:
        logger.info("Initializing FAQ retriever")
        faq_retriever_instance = FAQRetriever(force_rebuild=force_rebuild)
    return faq_retriever_instance


def retrieve_faq_answer(question: str, threshold=0.5) -> str:
    """
    Retrieve FAQ answer using RAG
    
    Args:
        question: User's procedural question
        threshold: Minimum similarity score (default 0.5 for high confidence)
    
    Returns:
        Answer string
    """
    try:
        retriever = get_faq_retriever()
        results = retriever.search(question, k=1, threshold=threshold)
        
        if results:
            best_match = results[0]
            logger.info(f"FAQ match: {best_match['id']} (score: {best_match['score']})")
            
            # High confidence match
            if best_match['score'] >= 0.7:
                return best_match['answer']
            
            # Medium confidence - provide answer with caveat
            elif best_match['score'] >= threshold:
                return (
                    f"{best_match['answer']}\n\n"
                    f"If this doesn't fully answer your question, please call 311 for more specific guidance."
                )
        
        # No good match found - generic fallback
        logger.info("No FAQ match found above threshold")
        return (
            "For information about Boston 311 services and procedures, "
            "please call 311 or visit boston.gov/311. "
            "A representative can provide specific guidance for your situation."
        )
        
    except Exception as e:
        logger.error(f"FAQ retrieval failed: {e}")
        return (
            "For assistance with Boston 311 services, "
            "please call 311 or visit boston.gov/311."
        )
