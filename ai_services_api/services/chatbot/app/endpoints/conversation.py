from fastapi import APIRouter, HTTPException, BackgroundTasks, Request, Depends
from typing import Optional, Dict, List
from pydantic import BaseModel
from datetime import datetime
import logging
from slowapi import Limiter
from slowapi.util import get_remote_address
import time
from ai_services_api.services.chatbot.utils.llm_manager import GeminiLLMManager
from ai_services_api.services.chatbot.utils.message_handler import MessageHandler
from ai_services_api.services.chatbot.utils.db_utils import DatabaseConnector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)

# Initialize managers
llm_manager = GeminiLLMManager()
message_handler = MessageHandler(llm_manager)
db_connector = DatabaseConnector()

# Response Models
class ContentMatchMetrics(BaseModel):
    content_id: str
    content_type: str
    title: str
    similarity_score: float
    rank: int
    clicked: bool = False

class ChatMetrics(BaseModel):
    session_id: str
    total_interactions: int
    avg_response_time: float
    success_rate: float
    content_matches: Dict[str, List[ContentMatchMetrics]]

class ChatResponse(BaseModel):
    response: str
    timestamp: datetime
    user_id: str
    session_id: str
    metrics: Optional[Dict] = None

# User ID dependencies
async def get_user_id(request: Request) -> str:
    """Get user ID from request header for production use"""
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")
    return user_id

async def get_test_user_id(request: Request) -> str:
    """Get user ID from request header or use default for testing"""
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        user_id = "test_user_123"
    return user_id

async def process_chat_request(
    query: str,
    user_id: str,
    background_tasks: BackgroundTasks
) -> ChatResponse:
    """Common chat processing logic with sentiment analysis"""
    db_conn = db_connector.get_connection()
    cursor = db_conn.cursor()
    start_time = datetime.utcnow()
    
    try:
        # Create a new session
        session_id = await message_handler.start_chat_session(user_id)
        
        # Get sentiment analysis
        sentiment_data = await llm_manager.analyze_sentiment(query)
        
        # Process message and collect response
        response_parts = []
        response_metadata = None
        
        async for part in message_handler.send_message_async(
            message=query,
            user_id=user_id,
            session_id=session_id
        ):
            if isinstance(part, dict) and part.get('is_metadata'):
                response_metadata = part.get('metadata')
            else:
                if isinstance(part, bytes):
                    part = part.decode('utf-8')
                response_parts.append(part)
        
        complete_response = ''.join(response_parts)
        
        # Record interaction and update session
        if response_metadata:
            # Add sentiment data to metadata
            response_metadata['sentiment'] = sentiment_data
            
            interaction_id = await message_handler.record_interaction(
                session_id=session_id,
                user_id=user_id,
                query=query,
                response_data={
                    'response': complete_response,
                    'metrics': {
                        'sentiment_score': sentiment_data['sentiment_score'],
                        'aspects': sentiment_data['aspects'],
                        'emotion_labels': sentiment_data['emotion_labels'],
                        **response_metadata.get('metrics', {})
                    },
                    **response_metadata
                }
            )
            
            # Update session with sentiment info
            await message_handler.update_session_stats(
                session_id=session_id,
                successful=not response_metadata.get('error_occurred', False),
                sentiment_score=sentiment_data['sentiment_score']
            )
            
            # Prepare metrics for response
            metrics = {
                'response_time': response_metadata.get('metrics', {}).get('response_time', 0.0),
                'intent': response_metadata.get('metrics', {}).get('intent', {}),
                'content_matches': response_metadata.get('metrics', {}).get('content_matches', {}),
                'sentiment': sentiment_data
            }
        else:
            metrics = None
        
        return ChatResponse(
            response=complete_response,
            timestamp=datetime.utcnow(),
            user_id=user_id,
            session_id=session_id,
            metrics=metrics
        )
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        if 'session_id' in locals():
            await message_handler.record_interaction(
                session_id=session_id,
                user_id=user_id,
                query=query,
                response_data={
                    'response': str(e),
                    'metrics': {
                        'sentiment_score': 0.0,
                        'aspects': {
                            'satisfaction': 0.0,
                            'urgency': 0.0,
                            'clarity': 0.0
                        },
                        'error_occurred': True
                    }
                }
            )
            await message_handler.update_session_stats(
                session_id=session_id,
                successful=False,
                sentiment_score=0.0
            )
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        db_conn.close()

# Test endpoint
@router.get("/test/chat/{query}")
@limiter.limit("5/minute")
async def test_chat_endpoint(
    query: str,
    request: Request,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_test_user_id)
):
    """Test chat endpoint with full analytics tracking."""
    return await process_chat_request(query, user_id, background_tasks)

# Production endpoint
@router.get("/chat/{query}")
@limiter.limit("5/minute")
async def chat_endpoint(
    query: str,
    request: Request,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(get_user_id)
):
    """Production chat endpoint with full analytics tracking."""
    return await process_chat_request(query, user_id, background_tasks)
