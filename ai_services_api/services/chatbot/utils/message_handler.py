import logging
import time
from typing import AsyncIterable, Optional, Dict
from .llm_manager import GeminiLLMManager
from .db_utils import DatabaseConnector

logger = logging.getLogger(__name__)

class MessageHandler:
    def __init__(self, llm_manager: GeminiLLMManager):
        self.llm_manager = llm_manager
        self.db_connector = DatabaseConnector()

    async def _get_db_connection(self):
        """Get a fresh database connection."""
        return self.db_connector.get_connection()

    async def start_chat_session(self, user_id: str) -> str:
        """Start a new chat session."""
        db_conn = await self._get_db_connection()
        cursor = db_conn.cursor()
        try:
            session_id = f"session_{int(time.time())}"
            cursor.execute("""
                INSERT INTO chat_sessions (session_id, user_id)
                VALUES (%s, %s)
                RETURNING session_id
            """, (session_id, user_id))
            db_conn.commit()
            return session_id
        except Exception as e:
            db_conn.rollback()
            logger.error(f"Error starting chat session: {e}")
            raise
        finally:
            cursor.close()
            db_conn.close()
            
    async def update_session_stats(self, session_id: str, successful: bool = True):
        """Update session statistics."""
        db_conn = await self._get_db_connection()
        cursor = db_conn.cursor()
        try:
            cursor.execute("""
                UPDATE chat_sessions 
                SET total_messages = total_messages + 1,
                    successful = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE session_id = %s
            """, (successful, session_id))
            db_conn.commit()
        except Exception as e:
            db_conn.rollback()
            logger.error(f"Error updating session stats: {e}")
            raise
        finally:
            cursor.close()
            db_conn.close()
            
    async def record_interaction(self, session_id: str, user_id: str, 
                               query: str, response_data: dict):
        """Record chat interaction and analytics."""
        db_conn = await self._get_db_connection()
        cursor = db_conn.cursor()
        try:
            metrics = response_data.get('metrics', {})
            
            # Get content matches count based on type
            content_matches = metrics.get('content_types', {})
            navigation_matches = content_matches.get('navigation', 0)
            publication_matches = content_matches.get('publication', 0)
            
            # Record interaction
            cursor.execute("""
                INSERT INTO chat_interactions 
                (session_id, user_id, query, response, timestamp, 
                response_time, intent_type, intent_confidence, 
                navigation_matches, publication_matches, error_occurred)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                session_id, 
                user_id, 
                query, 
                response_data.get('response', ''),
                metrics.get('response_time', 0.0),
                metrics.get('intent', {}).get('type', 'general'),
                metrics.get('intent', {}).get('confidence', 0.0),
                navigation_matches,
                publication_matches,
                response_data.get('error_occurred', False)
            ))
            
            interaction_id = cursor.fetchone()[0]
            
            # Record sentiment metrics if available
            sentiment = metrics.get('sentiment', {})
            if sentiment and isinstance(sentiment, dict):
                cursor.execute("""
                    INSERT INTO sentiment_metrics 
                    (interaction_id, sentiment_score, emotion_labels, 
                    satisfaction_score, urgency_score, clarity_score)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    interaction_id,
                    sentiment.get('score', 0.0),
                    sentiment.get('emotions', ['neutral']),
                    sentiment.get('aspects', {}).get('satisfaction', 0.0),
                    sentiment.get('aspects', {}).get('urgency', 0.0),
                    sentiment.get('aspects', {}).get('clarity', 0.0)
                ))
            
            # Record content matches in analytics
            content_matches = metrics.get('content_matches', [])
            if isinstance(content_matches, list):
                for match in content_matches:
                    if isinstance(match, dict):
                        cursor.execute("""
                            INSERT INTO chat_analytics 
                            (interaction_id, content_id, content_type, 
                            similarity_score, rank_position, clicked)
                            VALUES (%s, %s, %s, %s, %s, false)
                        """, (
                            interaction_id, 
                            match.get('id', 'unknown'),
                            match.get('type', 'unknown'),
                            match.get('similarity_score', 0.0),
                            match.get('rank_position', 0)
                        ))
            
            db_conn.commit()
            return interaction_id
            
        except Exception as e:
            db_conn.rollback()
            logger.error(f"Error recording interaction: {e}")
            raise
        finally:
            cursor.close()
            db_conn.close()
    
    async def send_message_async(
        self, 
        message: str,
        user_id: str,
        session_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        context: Optional[Dict] = None
    ) -> AsyncIterable[str]:
        """Process message and handle both chunks and metadata."""
        try:
            metadata = None
            async for response in self.llm_manager.generate_async_response(message):
                if response.get('is_metadata'):
                    metadata = response['metadata']
                    continue
                
                yield response['chunk']
            
            if metadata and session_id:
                try:
                    await self.record_interaction(
                        session_id=session_id,
                        user_id=user_id,
                        query=message,
                        response_data=metadata
                    )
                    
                    await self.update_session_stats(
                        session_id=session_id,
                        successful=not metadata.get('error_occurred', False)
                    )
                except Exception as e:
                    logger.error(f"Error recording message interaction: {e}")
                    # Continue with message delivery even if recording fails
                
        except Exception as e:
            logger.error(f"Error in send_message_async: {e}")
            error_message = "I apologize, but I encountered an error. Please try again."
            yield error_message.encode('utf-8', errors='replace')
            
            if session_id:
                try:
                    await self.record_interaction(
                        session_id=session_id,
                        user_id=user_id,
                        query=message,
                        response_data={
                            'response': error_message,
                            'intent_type': None,
                            'intent_confidence': 0.0,
                            'expert_matches': [],
                            'response_time': 0.0,
                            'error_occurred': True
                        }
                    )
                    await self.update_session_stats(
                        session_id=session_id,
                        successful=False
                    )
                except Exception as record_error:
                    logger.error(f"Error recording error state: {record_error}")

    async def flush_conversation_cache(self, conversation_id: str):
        """Clears the conversation history stored in the memory."""
        try:
            memory = self.llm_manager.create_memory()
            memory.clear()
            logger.info(f"Successfully flushed conversation cache for ID: {conversation_id}")
        except Exception as e:
            logger.error(f"Error while flushing conversation cache for ID {conversation_id}: {e}")
            raise RuntimeError(f"Failed to clear conversation history: {str(e)}")
