"""
Conversation Manager - Handles multi-turn conversations with context
"""
import logging
from typing import List, Dict, Optional
from datetime import datetime
from collections import deque

logger = logging.getLogger("b311.conversation")


class ConversationManager:
    """Manages conversation history and context for follow-up questions"""
    
    def __init__(self, max_history: int = 10):
        self.conversations: Dict[str, deque] = {}
        self.max_history = max_history
    
    def add_turn(self, session_id: str, question: str, answer: str, 
                 sql: Optional[str] = None, data: Optional[List] = None):
        """Add a conversation turn to history"""
        if session_id not in self.conversations:
            self.conversations[session_id] = deque(maxlen=self.max_history)
        
        turn = {
            "timestamp": datetime.utcnow().isoformat(),
            "question": question,
            "answer": answer,
            "sql": sql,
            "has_data": bool(data and len(data) > 0)
        }
        
        self.conversations[session_id].append(turn)
        logger.info(f"Added turn to session {session_id}. History length: {len(self.conversations[session_id])}")
    
    def get_context(self, session_id: str, num_turns: int = 3) -> str:
        """Get formatted conversation context for the LLM"""
        if session_id not in self.conversations:
            return ""
        
        history = list(self.conversations[session_id])[-num_turns:]
        
        if not history:
            return ""
        
        context_parts = ["Previous conversation context:"]
        for i, turn in enumerate(history, 1):
            context_parts.append(f"\nTurn {i}:")
            context_parts.append(f"  User: {turn['question']}")
            context_parts.append(f"  Assistant: {turn['answer'][:200]}...")
            if turn['sql']:
                context_parts.append(f"  SQL executed: {turn['sql'][:100]}...")
        
        return "\n".join(context_parts)
    
    def get_last_entities(self, session_id: str) -> Dict:
        """Get entities from the last query for context carryover"""
        if session_id not in self.conversations or not self.conversations[session_id]:
            return {}
        
        last_turn = self.conversations[session_id][-1]
        return {}
    
    def clear_session(self, session_id: str):
        """Clear conversation history for a session"""
        if session_id in self.conversations:
            del self.conversations[session_id]
            logger.info(f"Cleared session {session_id}")


# Global instance
conversation_manager = ConversationManager()


def get_conversation_manager() -> ConversationManager:
    """Get the global conversation manager instance"""
    return conversation_manager