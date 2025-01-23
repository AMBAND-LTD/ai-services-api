from fastapi import APIRouter, HTTPException, Request, Depends 
from typing import Optional, Dict 
from pydantic import BaseModel 
from datetime import datetime 
import logging
from slowapi import Limiter
from slowapi.util import get_remote_address
from ai_services_api.services.chatbot.utils.llm_manager import GeminiLLMManager
from ai_services_api.services.chatbot.utils.message_handler import MessageHandler
from ai_services_api.services.message.core.database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)

llm_manager = GeminiLLMManager()
message_handler = MessageHandler(llm_manager)

class ChatResponse(BaseModel):
    response: str
    timestamp: datetime
    user_id: str

async def get_test_user_id(request: Request) -> str:
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        user_id = "test_user_123"
    return user_id

async def get_user_id(request: Request) -> str:
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")
    return user_id

async def record_interaction(user_id: str, query: str, response: str, metrics: Dict = None):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO interactions
                (session_id, user_id, query, response, metrics, timestamp)
            VALUES 
                (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            f"session_{user_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            user_id,
            query,
            response,
            json.dumps(metrics) if metrics else None
        ))
        
        interaction_id = cur.fetchone()[0]
        conn.commit()
        return interaction_id
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error recording interaction: {e}")
        return None
    finally:
        if conn:
            conn.close()

async def process_chat_request(query: str, user_id: str) -> ChatResponse:
    try:
        response_parts = []
        metrics = {
            "sentiment_score": 0.0,
            "aspects": {},
            "error_occurred": False,
            "response_time": None
        }
        start_time = datetime.utcnow()
        
        try:
            async for part in message_handler.send_message_async(
                message=query,
                user_id=user_id
            ):
                if isinstance(part, dict):
                    metrics.update(part)
                    continue
                if isinstance(part, bytes):
                    part = part.decode('utf-8')
                response_parts.append(part)
        except Exception as e:
            metrics["error_occurred"] = True
            raise e
        finally:
            metrics["response_time"] = (datetime.utcnow() - start_time).total_seconds()
                
        complete_response = ''.join(response_parts)
        
        await record_interaction(
            user_id=user_id,
            query=query,
            response=complete_response,
            metrics=metrics
        )
        
        return ChatResponse(
            response=complete_response,
            timestamp=datetime.utcnow(),
            user_id=user_id
        )
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/test/chat/{query}")
@limiter.limit("5/minute")
async def test_chat_endpoint(
    query: str,
    request: Request,
    user_id: str = Depends(get_test_user_id)
):
    return await process_chat_request(query, user_id)

@router.get("/chat/{query}")
@limiter.limit("5/minute") 
async def chat_endpoint(
    query: str,
    request: Request,
    user_id: str = Depends(get_user_id)
):
    return await process_chat_request(query, user_id)