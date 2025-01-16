# In your expert_routes.py for recommendations

from fastapi import APIRouter, HTTPException, BackgroundTasks, Request, Depends, Query
from enum import Enum
from typing import Optional

# Define interaction types
class InteractionType(str, Enum):
    SEARCH = "search"
    MESSAGE = "message"

@router.post("/test/record-interaction/{expert_id}")
async def record_test_interaction(
    expert_id: str,
    interaction_type: InteractionType,
    background_tasks: BackgroundTasks,
    query: Optional[str] = None,
    message_content: Optional[str] = None,
    receiver_id: Optional[str] = None,
    rank_position: Optional[int] = None,
    clicked: Optional[bool] = Query(False),
    user_id: str = Depends(get_test_user_id)
):
    """
    Test endpoint to record and simulate interactions with search and messaging.
    This will also trigger the recommendation system's weight adaptation.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Record interaction based on type
        if interaction_type == InteractionType.SEARCH:
            if not query:
                raise HTTPException(
                    status_code=400, 
                    detail="Query parameter required for search interaction"
                )

            # Record search interaction
            cur.execute("""
                INSERT INTO search_logs (
                    user_id,
                    query,
                    timestamp
                ) VALUES (%s, %s, NOW())
                RETURNING id
            """, (user_id, query))
            
            search_id = cur.fetchone()[0]

            # Record expert search result
            cur.execute("""
                INSERT INTO expert_searches (
                    search_id,
                    expert_id,
                    rank_position,
                    clicked
                ) VALUES (%s, %s, %s, %s)
            """, (
                search_id,
                expert_id,
                rank_position or 1,
                clicked
            ))

            response_data = {
                "interaction_type": "search",
                "search_id": search_id,
                "query": query,
                "expert_id": expert_id,
                "clicked": clicked
            }

        elif interaction_type == InteractionType.MESSAGE:
            if not message_content or not receiver_id:
                raise HTTPException(
                    status_code=400,
                    detail="message_content and receiver_id required for message interaction"
                )

            # Record message interaction
            cur.execute("""
                INSERT INTO expert_interactions (
                    sender_id,
                    receiver_id,
                    interaction_type,
                    success,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s::jsonb)
                RETURNING id
            """, (
                expert_id,
                receiver_id,
                'message',
                True,
                json.dumps({
                    'content_length': len(message_content),
                    'is_test': True
                })
            ))
            
            interaction_id = cur.fetchone()[0]

            response_data = {
                "interaction_type": "message",
                "interaction_id": interaction_id,
                "sender_id": expert_id,
                "receiver_id": receiver_id,
                "content_length": len(message_content)
            }

        # Commit transaction
        conn.commit()

        # Trigger weight adaptation in background
        background_tasks.add_task(
            expert_service._adjust_weights,
            expert_id
        )

        return {
            "status": "success",
            "interaction_recorded": response_data,
            "timestamp": datetime.utcnow()
        }

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error recording test interaction: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error recording interaction: {str(e)}"
        )
    finally:
        if conn:
            conn.close()