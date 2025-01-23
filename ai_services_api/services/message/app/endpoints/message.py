from fastapi import APIRouter, HTTPException, Request, Depends
from typing import List, Optional, Dict
from ai_services_api.services.message.core.database import get_db_connection
from ai_services_api.services.message.core.config import get_settings
import google.generativeai as genai
from datetime import datetime
import logging
import json
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

router = APIRouter()

async def get_user_id(request: Request) -> str:
   user_id = request.headers.get("X-User-ID")
   if not user_id:
       raise HTTPException(status_code=400, detail="X-User-ID header is required")
   return user_id

async def get_test_user_id(request: Request) -> str:
   user_id = request.headers.get("X-User-ID")
   if not user_id:
       user_id = "123"
   return user_id

async def record_interaction_metrics(cur, interaction_id: int, metadata: Dict):
   try:
       cur.execute("""
           INSERT INTO interactions
               (session_id, interaction_id, metrics, timestamp)
           VALUES 
               (%s, %s, %s, CURRENT_TIMESTAMP)
       """, (
           f"msg_{interaction_id}",
           interaction_id,
           json.dumps(metadata)
       ))
   except Exception as e:
       logger.error(f"Error recording metrics: {e}")

async def record_expert_interaction(
   cur,
   sender_id: int,
   receiver_id: int,
   interaction_type: str,
   metadata: Dict = None
):
   try:
       cur.execute("""
           SELECT theme, domains, fields 
           FROM experts_expert 
           WHERE id = %s
       """, (sender_id,))
       sender_details = cur.fetchone()

       cur.execute("""
           SELECT theme, domains, fields 
           FROM experts_expert 
           WHERE id = %s
       """, (receiver_id,))
       receiver_details = cur.fetchone()

       interaction_metadata = {
           "sender": {
               "theme": sender_details['theme'] if sender_details else None,
               "domains": sender_details['domains'] if sender_details else [],
               "fields": sender_details['fields'] if sender_details else []
           },
           "receiver": {
               "theme": receiver_details['theme'] if receiver_details else None,
               "domains": receiver_details['domains'] if receiver_details else [],
               "fields": receiver_details['fields'] if receiver_details else []
           }
       }

       if metadata:
           interaction_metadata.update(metadata)

       cur.execute("""
           INSERT INTO expert_interactions 
               (sender_id, receiver_id, interaction_type, metadata, created_at)
           VALUES 
               (%s, %s, %s, %s, CURRENT_TIMESTAMP)
           RETURNING id
       """, (sender_id, receiver_id, interaction_type, json.dumps(interaction_metadata)))

       return cur.fetchone()['id']

   except Exception as e:
       logger.error(f"Error recording expert interaction: {str(e)}")
       return None

async def process_message_draft(
   user_id: str,
   receiver_id: str, 
   content: str
):
   logger.info(f"Creating draft message to expert {receiver_id}")
   
   conn = None
   cur = None
   start_time = datetime.utcnow()
   
   try:
       conn = get_db_connection()
       cur = conn.cursor(cursor_factory=RealDictCursor)
           
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

       settings = get_settings()
       genai.configure(api_key=settings.GEMINI_API_KEY)
       model = genai.GenerativeModel('gemini-pro')
       
       prompt = f"""
       Draft a professional message to {receiver['first_name']} {receiver['last_name']} ({receiver['designation'] or 'Expert'}).
       
       Context about receiver:
       - Theme: {receiver['theme'] or 'Not specified'}
       - Domains: {', '.join(receiver['domains'] if receiver.get('domains') else ['Not specified'])}
       - Fields: {', '.join(receiver['fields'] if receiver.get('fields') else ['Not specified'])}
       
       Additional context: {content}
       """
       
       response = model.generate_content(prompt)
       draft_content = response.text

       cur.execute("""
           INSERT INTO expert_messages 
               (sender_id, receiver_id, content, draft, created_at, updated_at) 
           VALUES 
               (%s, %s, %s, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
           RETURNING id, created_at
       """, (1, receiver_id, draft_content))
       
       new_message = cur.fetchone()

       interaction_metadata = {
           "message_id": new_message['id'],
           "content_length": len(draft_content),
           "context": content
       }
       
       interaction_id = await record_expert_interaction(
           cur,
           sender_id=1,
           receiver_id=int(receiver_id),
           interaction_type='message_draft',
           metadata=interaction_metadata
       )

       metrics = {
           "response_time": (datetime.utcnow() - start_time).total_seconds(),
           "content_length": len(draft_content),
           "error_occurred": False,
           "interaction_id": interaction_id
       }
       
       await record_interaction_metrics(cur, new_message['id'], metrics)
       conn.commit()

       return {
           "id": str(new_message['id']),
           "content": draft_content,
           "sender_id": user_id,
           "receiver_id": str(receiver_id),
           "created_at": new_message['created_at'],
           "draft": True,
           "receiver_name": f"{receiver['first_name']} {receiver['last_name']}",
           "sender_name": "Test User"
       }

   except Exception as e:
       if conn:
           conn.rollback()
       logger.error(f"Error in process_message_draft: {str(e)}")
       raise HTTPException(status_code=500, detail=str(e))
   finally:
       if cur:
           cur.close()
       if conn:
           conn.close()

@router.get("/analytics/interactions")
async def get_expert_interactions(
   request: Request,
   start_date: Optional[str] = None,
   end_date: Optional[str] = None,
   interaction_type: Optional[str] = None
):
   conn = None
   cur = None
   try:
       conn = get_db_connection()
       cur = conn.cursor(cursor_factory=RealDictCursor)

       query = """
           SELECT 
               ei.sender_id,
               ei.receiver_id,
               ei.interaction_type,
               ei.created_at,
               ei.metadata,
               s.first_name as sender_first_name,
               s.last_name as sender_last_name,
               r.first_name as receiver_first_name,
               r.last_name as receiver_last_name,
               i.metrics as interaction_metrics
           FROM expert_interactions ei
           JOIN experts_expert s ON ei.sender_id = s.id
           JOIN experts_expert r ON ei.receiver_id = r.id
           LEFT JOIN interactions i ON i.interaction_id = ei.id
           WHERE 1=1
       """
       params = []

       if start_date:
           query += " AND ei.created_at >= %s"
           params.append(start_date)
       
       if end_date:
           query += " AND ei.created_at <= %s"
           params.append(end_date)
           
       if interaction_type:
           query += " AND ei.interaction_type = %s"
           params.append(interaction_type)

       query += " ORDER BY ei.created_at DESC"
       
       cur.execute(query, params)
       interactions = cur.fetchall()

       return {
           "total_interactions": len(interactions),
           "interactions": interactions
       }

   except Exception as e:
       logger.error(f"Error fetching expert interactions: {str(e)}")
       raise HTTPException(status_code=500, detail=str(e))
   finally:
       if cur:
           cur.close()
       if conn:
           conn.close()

@router.get("/test/draft/{receiver_id}/{content}")
async def test_create_message_draft(
   receiver_id: str,
   content: str,
   request: Request,
   user_id: str = Depends(get_test_user_id)
):
   return await process_message_draft(user_id, receiver_id, content)

@router.get("/draft/{receiver_id}/{content}")
async def create_message_draft(
   receiver_id: str,
   content: str,
   request: Request,
   user_id: str = Depends(get_user_id)
):
   return await process_message_draft(user_id, receiver_id, content)