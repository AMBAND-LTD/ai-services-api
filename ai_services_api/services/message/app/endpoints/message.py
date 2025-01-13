# app/api/endpoints/messages.py
from fastapi import APIRouter, HTTPException, Request, Depends
from typing import List, Optional
from ai_services_api.services.message.core.database import get_db_connection
from ai_services_api.services.message.core.config import get_settings
import google.generativeai as genai
from datetime import datetime
import logging
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter()

# User ID dependencies
async def get_user_id(request: Request) -> int:
    """Get user ID from request header for production use"""
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")
    return int(user_id)

async def get_test_user_id(request: Request) -> int:
    """Get user ID from request header or use default for testing"""
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        user_id = "123"  # Default test user ID
    return int(user_id)

async def process_message_draft(
    sender_id: int,
    receiver_id: int,
    content: str
):
    """Common message draft processing logic"""
    logger.info(f"Creating draft message from expert {sender_id} to expert {receiver_id}")
    
    conn = get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get sender and receiver details
        cur.execute("""
            SELECT id, first_name, last_name, designation, theme, domains, fields 
            FROM experts_expert 
            WHERE id = %s AND is_active = true
        """, (sender_id,))
        sender = cur.fetchone()
        
        if not sender:
            raise HTTPException(
                status_code=404,
                detail=f"Sender with ID {sender_id} not found or is inactive"
            )
            
        cur.execute("""
            SELECT id, first_name, last_name, designation, theme, domains, fields 
            FROM experts_expert 
            WHERE id = %s AND is_active = true
        """, (receiver_id,))
        receiver = cur.fetchone()
        
        if not receiver:
            raise HTTPException(
                status_code=404,
                detail=f"Receiver with ID {receiver_id} not found or is inactive"
            )

        # Generate AI content
        settings = get_settings()
        genai.configure(api_key=settings.GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-pro')
        
        prompt = f"""
        Draft a professional message from {sender['first_name']} {sender['last_name']} ({sender['designation'] or 'Expert'}) 
        to {receiver['first_name']} {receiver['last_name']} ({receiver['designation'] or 'Expert'}).
        
        Context about sender:
        - Theme: {sender['theme'] or 'Not specified'}
        - Domains: {', '.join(sender['domains'] if sender.get('domains') else ['Not specified'])}
        - Fields: {', '.join(sender['fields'] if sender.get('fields') else ['Not specified'])}
        
        Context about receiver:
        - Theme: {receiver['theme'] or 'Not specified'}
        - Domains: {', '.join(receiver['domains'] if receiver.get('domains') else ['Not specified'])}
        - Fields: {', '.join(receiver['fields'] if receiver.get('fields') else ['Not specified'])}
        
        Additional context: {content}
        """
        
        response = model.generate_content(prompt)
        draft_content = response.text

        # Save draft message
        cur.execute("""
            INSERT INTO expert_messages 
                (sender_id, receiver_id, content, draft, created_at, updated_at) 
            VALUES 
                (%s, %s, %s, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, created_at
        """, (sender_id, receiver_id, draft_content))
        
        new_message = cur.fetchone()
        conn.commit()

        return {
            "id": new_message['id'],
            "content": draft_content,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "created_at": new_message['created_at'],
            "draft": True,
            "sender_name": f"{sender['first_name']} {sender['last_name']}",
            "receiver_name": f"{receiver['first_name']} {receiver['last_name']}"
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"Error in process_message_draft: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

# Test endpoints
@router.post("/test/draft")
async def test_create_message_draft(
    sender_id: int,
    receiver_id: int,
    content: str,
    request: Request
):
    """Test endpoint for creating draft messages"""
    return await process_message_draft(sender_id, receiver_id, content)

@router.put("/test/messages/{message_id}")
async def test_update_message(
    message_id: int,
    content: str,
    mark_as_sent: Optional[bool] = False
):
    """Test endpoint for updating messages"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            UPDATE expert_messages 
            SET content = %s,
                draft = CASE WHEN %s THEN false ELSE draft END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING *
        """, (content, mark_as_sent, message_id))
        
        updated_message = cur.fetchone()
        if not updated_message:
            raise HTTPException(status_code=404, detail="Message not found")
            
        conn.commit()
        return updated_message
    finally:
        cur.close()
        conn.close()

@router.delete("/test/messages/{message_id}")
async def test_delete_message(message_id: int):
    """Test endpoint for deleting messages"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("DELETE FROM expert_messages WHERE id = %s RETURNING id", (message_id,))
        deleted = cur.fetchone()
        
        if not deleted:
            raise HTTPException(status_code=404, detail="Message not found")
            
        conn.commit()
        return {"message": "Message deleted successfully"}
    finally:
        cur.close()
        conn.close()

@router.get("/test/messages/thread/{other_user_id}")
async def test_get_message_thread(
    user_id: int,
    other_user_id: int,
    limit: int = 50
):
    """Test endpoint for getting message threads"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cur.execute("""
            SELECT m.*, 
                   s.first_name as sender_first_name, 
                   s.last_name as sender_last_name,
                   r.first_name as receiver_first_name, 
                   r.last_name as receiver_last_name
            FROM expert_messages m
            JOIN experts_expert s ON m.sender_id = s.id
            JOIN experts_expert r ON m.receiver_id = r.id
            WHERE (m.sender_id = %s AND m.receiver_id = %s)
               OR (m.sender_id = %s AND m.receiver_id = %s)
            ORDER BY m.created_at DESC
            LIMIT %s
        """, (user_id, other_user_id, other_user_id, user_id, limit))
        
        messages = cur.fetchall()
        
        return [{
            "id": msg['id'],
            "content": msg['content'],
            "sender_id": msg['sender_id'],
            "receiver_id": msg['receiver_id'],
            "sender_name": f"{msg['sender_first_name']} {msg['sender_last_name']}",
            "receiver_name": f"{msg['receiver_first_name']} {msg['receiver_last_name']}",
            "created_at": msg['created_at'],
            "draft": msg['draft']
        } for msg in messages]
    finally:
        cur.close()
        conn.close()

# Production endpoints
@router.post("/draft")
async def create_message_draft(
    request: Request,
    sender_id: int,
    receiver_id: int,
    content: str,
    user_id: int = Depends(get_user_id)
):
    """Production endpoint for creating draft messages"""
    if user_id != sender_id:
        raise HTTPException(
            status_code=403,
            detail="User ID in header must match sender ID"
        )
    return await process_message_draft(sender_id, receiver_id, content)

@router.put("/messages/{message_id}")
async def update_message(
    request: Request,
    message_id: int,
    content: str,
    user_id: int = Depends(get_user_id),
    mark_as_sent: Optional[bool] = False
):
    """Production endpoint for updating messages"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Verify message ownership
        cur.execute("SELECT sender_id FROM expert_messages WHERE id = %s", (message_id,))
        message = cur.fetchone()
        if not message or message['sender_id'] != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to update this message")
        
        cur.execute("""
            UPDATE expert_messages 
            SET content = %s,
                draft = CASE WHEN %s THEN false ELSE draft END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING *
        """, (content, mark_as_sent, message_id))
        
        updated_message = cur.fetchone()
        conn.commit()
        return updated_message
    finally:
        cur.close()
        conn.close()

@router.delete("/messages/{message_id}")
async def delete_message(
    request: Request,
    message_id: int,
    user_id: int = Depends(get_user_id)
):
    """Production endpoint for deleting messages"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Verify message ownership
        cur.execute("SELECT sender_id FROM expert_messages WHERE id = %s", (message_id,))
        message = cur.fetchone()
        if not message or message['sender_id'] != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to delete this message")
        
        cur.execute("DELETE FROM expert_messages WHERE id = %s RETURNING id", (message_id,))
        deleted = cur.fetchone()
        conn.commit()
        return {"message": "Message deleted successfully"}
    finally:
        cur.close()
        conn.close()

@router.get("/messages/thread/{other_user_id}")
async def get_message_thread(
    request: Request,
    other_user_id: int,
    user_id: int = Depends(get_user_id),
    limit: int = 50
):
    """Production endpoint for getting message threads"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cur.execute("""
            SELECT m.*, 
                   s.first_name as sender_first_name, 
                   s.last_name as sender_last_name,
                   r.first_name as receiver_first_name, 
                   r.last_name as receiver_last_name
            FROM expert_messages m
            JOIN experts_expert s ON m.sender_id = s.id
            JOIN experts_expert r ON m.receiver_id = r.id
            WHERE (m.sender_id = %s AND m.receiver_id = %s)
               OR (m.sender_id = %s AND m.receiver_id = %s)
            ORDER BY m.created_at DESC
            LIMIT %s
        """, (user_id, other_user_id, other_user_id, user_id, limit))
        
        messages = cur.fetchall()
        
        return [{
            "id": msg['id'],
            "content": msg['content'],
            "sender_id": msg['sender_id'],
            "receiver_id": msg['receiver_id'],
            "sender_name": f"{msg['sender_first_name']} {msg['sender_last_name']}",
            "receiver_name": f"{msg['receiver_first_name']} {msg['receiver_last_name']}",
            "created_at": msg['created_at'],
            "draft": msg['draft']
        } for msg in messages]
    finally:
        cur.close()
        conn.close()