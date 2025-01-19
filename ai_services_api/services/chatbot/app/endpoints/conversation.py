from fastapi import APIRouter, HTTPException, Request, Depends
from typing import Optional, Dict
from pydantic import BaseModel
from datetime import datetime
import logging
from slowapi import Limiter
from slowapi.util import get_remote_address
from ai_services_api.services.chatbot.utils.llm_manager import GeminiLLMManager
from ai_services_api.services.chatbot.utils.message_handler import MessageHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)

# Initialize managers
llm_manager = GeminiLLMManager()
message_handler = MessageHandler(llm_manager)

class ChatResponse(BaseModel):
    response: str
    timestamp: datetime
    user_id: str

async def get_test_user_id(request: Request) -> str:
    """Get user ID from request header or use default for testing"""
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        user_id = "test_user_123"
    return user_id

async def get_user_id(request: Request) -> str:
    """Get user ID from request header for production use"""
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")
    return user_id

async def process_chat_request(query: str, user_id: str) -> ChatResponse:
    """Simplified chat processing logic - just gets the response"""
    try:
        response_parts = []
        
        async for part in message_handler.send_message_async(
            message=query,
            user_id=user_id
        ):
            if isinstance(part, dict):
                continue  # Skip metadata
            if isinstance(part, bytes):
                part = part.decode('utf-8')
            response_parts.append(part)
        
        complete_response = ''.join(response_parts)
        
        return ChatResponse(
            response=complete_response,
            timestamp=datetime.utcnow(),
            user_id=user_id
        )
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Test endpoint
@router.get("/test/chat/{query}")
@limiter.limit("5/minute")
async def test_chat_endpoint(
    query: str,
    request: Request,
    user_id: str = Depends(get_test_user_id)
):
    """Simplified test chat endpoint."""
    return await process_chat_request(query, user_id)

# Production endpoint
@router.get("/chat/{query}")
@limiter.limit("5/minute")
async def chat_endpoint(
    query: str,
    request: Request,
    user_id: str = Depends(get_user_id)
):
    """Simplified production chat endpoint."""
    return await process_chat_request(query, user_id)