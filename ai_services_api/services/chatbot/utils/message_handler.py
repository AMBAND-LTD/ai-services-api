import logging
import time
from typing import AsyncIterable, Optional, Dict
from .llm_manager import GeminiLLMManager
from .db_utils import DatabaseConnector
from ai_services_api.services.message.core.database import get_db_connection


logger = logging.getLogger(__name__)

class MessageHandler:
    def __init__(self, llm_manager: GeminiLLMManager):
        """
        Initialize MessageHandler with LLM manager.
        
        Args:
            llm_manager (GeminiLLMManager): Language model management instance
        """
        self.llm_manager = llm_manager

    async def start_chat_session(self, user_id: str) -> str:
        """
        Start a new chat session.
        
        Args:
            user_id (str): Unique identifier for the user
        
        Returns:
            str: Generated session identifier
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Generate unique session identifier
                    session_id = f"session_{user_id}_{int(time.time())}"
                    
                    try:
                        cursor.execute("""
                            INSERT INTO chat_sessions 
                                (session_id, user_id, start_timestamp)
                            VALUES (%s, %s, CURRENT_TIMESTAMP)
                            RETURNING session_id
                        """, (session_id, user_id))
                        
                        conn.commit()
                        logger.info(f"Created chat session: {session_id}")
                        return session_id
                    
                    except Exception as insert_error:
                        conn.rollback()
                        logger.error(f"Error inserting chat session: {insert_error}")
                        raise
        
        except Exception as e:
            logger.error(f"Error in start_chat_session: {e}")
            raise

    async def record_interaction(self, session_id: str, user_id: str, 
                                 query: str, response_data: dict):
        """
        Record detailed chat interaction with comprehensive analytics.
        
        Args:
            session_id (str): Chat session identifier
            user_id (str): User identifier
            query (str): User's query
            response_data (dict): Comprehensive response and metrics information
        
        Returns:
            int: Recorded interaction ID
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        metrics = response_data.get('metrics', {})
                        
                        # Record the main interaction
                        cursor.execute("""
                            INSERT INTO chat_interactions 
                                (session_id, user_id, query, response, 
                                response_time, intent_type, intent_confidence, 
                                navigation_matches, publication_matches, error_occurred)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING id
                        """, (
                            session_id,
                            user_id,
                            query,
                            response_data.get('response', ''),
                            metrics.get('response_time', 0.0),
                            metrics.get('intent', {}).get('type', 'general'),
                            metrics.get('intent', {}).get('confidence', 0.0),
                            metrics.get('content_types', {}).get('navigation', 0),
                            metrics.get('content_types', {}).get('publication', 0),
                            response_data.get('error_occurred', False)
                        ))
                        
                        interaction_id = cursor.fetchone()[0]
                        
                        # Record sentiment metrics
                        sentiment = metrics.get('sentiment', {})
                        if sentiment:
                            cursor.execute("""
                                INSERT INTO sentiment_metrics 
                                    (interaction_id, sentiment_score, emotion_labels,
                                    satisfaction_score, urgency_score, clarity_score)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                interaction_id,
                                sentiment.get('sentiment_score', 0.0),
                                sentiment.get('emotion_labels', ['neutral']),
                                sentiment.get('aspects', {}).get('satisfaction', 0.0),
                                sentiment.get('aspects', {}).get('urgency', 0.0),
                                sentiment.get('aspects', {}).get('clarity', 0.0)
                            ))
                        
                        # Record content matches
                        content_matches = metrics.get('content_matches', [])
                        for match in content_matches:
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
                        
                        conn.commit()
                        logger.info(f"Recorded interaction for session {session_id}")
                        return interaction_id
                    
                    except Exception as insert_error:
                        conn.rollback()
                        logger.error(f"Error recording interaction: {insert_error}")
                        raise
        
        except Exception as e:
            logger.error(f"Error in record_interaction: {e}")
            raise

    async def update_session_stats(self, session_id: str, successful: bool = True):
        """
        Update session statistics after interaction.
        
        Args:
            session_id (str): Chat session identifier
            successful (bool): Whether the interaction was successful
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute("""
                            UPDATE chat_sessions 
                            SET total_messages = total_messages + 1,
                                successful = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE session_id = %s
                        """, (successful, session_id))
                        
                        conn.commit()
                        logger.info(f"Updated session stats for {session_id}")
                    
                    except Exception as update_error:
                        conn.rollback()
                        logger.error(f"Error updating session stats: {update_error}")
                        raise
        
        except Exception as e:
            logger.error(f"Error in update_session_stats: {e}")
            raise

    async def send_message_async(
        self, 
        message: str,
        user_id: str,
        session_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        context: Optional[Dict] = None
    ) -> AsyncIterable[str]:
        """
        Process message and handle both chunks and metadata.
        
        Args:
            message (str): User's input message
            user_id (str): User identifier
            session_id (Optional[str]): Existing session ID
            conversation_id (Optional[str]): Conversation identifier
            context (Optional[Dict]): Additional context for message processing
        
        Yields:
            str: Response chunks from the LLM
        """
        try:
            # Create new session if none provided
            if not session_id:
                session_id = await self.start_chat_session(user_id)

            # Initialize metadata buffer
            metadata = None
            response_chunks = []
            
            # Process message stream
            async for response in self.llm_manager.generate_async_response(message):
                if response.get('is_metadata'):
                    metadata = response['metadata']
                    continue
                
                chunk = response['chunk']
                if isinstance(chunk, bytes):
                    chunk = chunk.decode('utf-8')
                response_chunks.append(chunk)
                yield chunk
            
            # Record analytics after full response
            if metadata and session_id:
                try:
                    complete_response = ''.join(response_chunks)
                    await self.record_interaction(
                        session_id=session_id,
                        user_id=user_id,
                        query=message,
                        response_data={
                            'response': complete_response,
                            'metrics': metadata.get('metrics', {}),
                            'error_occurred': metadata.get('error_occurred', False)
                        }
                    )
                    
                    await self.update_session_stats(
                        session_id=session_id,
                        successful=not metadata.get('error_occurred', False)
                    )
                except Exception as e:
                    logger.error(f"Error recording message interaction: {e}")
                
        except Exception as e:
            logger.error(f"Error in send_message_async: {e}")
            error_message = "I apologize, but I encountered an error. Please try again."
            yield error_message
            
            if session_id:
                try:
                    await self.record_interaction(
                        session_id=session_id,
                        user_id=user_id,
                        query=message,
                        response_data={
                            'response': error_message,
                            'metrics': {
                                'response_time': 0.0,
                                'intent': {'type': 'error', 'confidence': 0.0},
                                'sentiment': {
                                    'sentiment_score': 0.0,
                                    'emotion_labels': ['error'],
                                    'aspects': {
                                        'satisfaction': 0.0,
                                        'urgency': 0.0,
                                        'clarity': 0.0
                                    }
                                },
                                'content_matches': [],
                                'content_types': {
                                    'navigation': 0,
                                    'publication': 0
                                }
                            },
                            'error_occurred': True
                        }
                    )
                    await self.update_session_stats(
                        session_id=session_id,
                        successful=False
                    )
                except Exception as record_error:
                    logger.error(f"Error recording error state: {record_error}")

    async def update_content_click(self, interaction_id: int, content_id: str):
        """
        Update when a user clicks on a content match.
        
        Args:
            interaction_id (int): Interaction identifier
            content_id (str): Clicked content identifier
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute("""
                            UPDATE chat_analytics 
                            SET clicked = true 
                            WHERE interaction_id = %s AND content_id = %s
                        """, (interaction_id, content_id))
                        conn.commit()
                    except Exception as update_error:
                        conn.rollback()
                        logger.error(f"Error updating content click: {update_error}")
                        raise
        except Exception as e:
            logger.error(f"Error in update_content_click: {e}")
            raise
            
    async def flush_conversation_cache(self, conversation_id: str):
        """Clears the conversation history stored in the memory."""
        try:
            memory = self.llm_manager.create_memory()
            memory.clear()
            logger.info(f"Successfully flushed conversation cache for ID: {conversation_id}")
        except Exception as e:
            logger.error(f"Error while flushing conversation cache for ID {conversation_id}: {e}")
            raise RuntimeError(f"Failed to clear conversation history: {str(e)}")
