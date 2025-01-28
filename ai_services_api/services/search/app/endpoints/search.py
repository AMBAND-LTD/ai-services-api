from fastapi import APIRouter, HTTPException, Request, Depends
from typing import List, Dict, Optional
from pydantic import BaseModel
import logging
from datetime import datetime, timezone
import json
import pandas as pd
import uuid  # Add this import at the top of the file


from ai_services_api.services.search.indexing.index_creator import ExpertSearchIndexManager
from ai_services_api.services.search.ml.ml_predictor import MLPredictor
from ai_services_api.services.message.core.database import get_db_connection

router = APIRouter()
logger = logging.getLogger(__name__)

# Constants
TEST_USER_ID = "123"

# Initialize ML Predictor
ml_predictor = MLPredictor()

# Response Models
class ExpertSearchResult(BaseModel):
    id: str
    first_name: str
    last_name: str
    designation: str
    theme: str
    unit: str
    contact: str
    is_active: bool
    score: float = None
    bio: str = None  
    knowledge_expertise: List[str] = []

class SearchResponse(BaseModel):
    total_results: int
    experts: List[ExpertSearchResult]
    user_id: str
    session_id: str

class PredictionResponse(BaseModel):
    predictions: List[str]
    confidence_scores: List[float]
    user_id: str

async def get_user_id(request: Request) -> str:
    """Get user ID from request header"""
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")
    return user_id

async def get_test_user_id(request: Request) -> str:
    """Get user ID from request header or use test ID"""
    return TEST_USER_ID

async def get_or_create_session(conn, user_id: str) -> str:
    """Get existing active session or create new one"""
    cur = conn.cursor()
    try:
        # Generate a numeric session ID using current timestamp 
        # but constraining it to a smaller integer range
        session_id = int(str(int(datetime.utcnow().timestamp()))[-8:])
        
        cur.execute("""
            INSERT INTO search_sessions 
                (session_id, user_id, start_timestamp, is_active)
            VALUES (%s, %s, CURRENT_TIMESTAMP, true)
            RETURNING session_id
        """, (session_id, user_id))
        
        conn.commit()
        return str(session_id)  # Return as string to match existing code
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating session: {e}")
        raise
    finally:
        cur.close()
async def record_search(conn, session_id: str, user_id: str, query: str, results: List[Dict], response_time: float):
    """Record search analytics"""
    cur = conn.cursor()
    try:
        # Update session stats
        cur.execute("""
            UPDATE search_sessions 
            SET query_count = query_count + 1,
                successful_searches = successful_searches + %s
            WHERE session_id = %s
        """, (1 if results else 0, session_id))

        # Record search analytics
        cur.execute("""
            INSERT INTO search_analytics
                (search_id, query, user_id, response_time, 
                result_count, search_type, timestamp)
            VALUES
                ((SELECT id FROM search_sessions WHERE session_id = %s),
                %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            session_id,
            query,
            user_id,
            response_time,
            len(results),
            'expert_search'
        ))
        
        search_id = cur.fetchone()[0]

        # Record expert matches
        for rank, result in enumerate(results, 1):
            cur.execute("""
                INSERT INTO expert_search_matches
                    (search_id, expert_id, rank_position, similarity_score)
                VALUES (%s, %s, %s, %s)
            """, (
                search_id,
                result["id"],
                rank,
                result.get("score", 0.0)
            ))

            # Update domain expertise analytics
            for domain in result.get('specialties', {}).get('domains', []):
                cur.execute("""
                    INSERT INTO domain_expertise_analytics 
                        (domain_name, match_count, last_matched_at)
                    VALUES (%s, 1, CURRENT_TIMESTAMP)
                    ON CONFLICT (domain_name) DO UPDATE SET
                        match_count = domain_expertise_analytics.match_count + 1,
                        last_matched_at = CURRENT_TIMESTAMP
                """, (domain,))

        conn.commit()
        return search_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Error recording search: {e}")
        raise
    finally:
        cur.close()

async def record_prediction(conn, session_id: str, user_id: str, partial_query: str, predictions: List[str], confidence_scores: List[float]):
    """Record prediction analytics"""
    cur = conn.cursor()
    try:
        # Record predictions
        for pred, conf in zip(predictions, confidence_scores):
            cur.execute("""
                INSERT INTO query_predictions
                    (partial_query, predicted_query, confidence_score, 
                    user_id, timestamp)
                VALUES 
                    (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (partial_query, pred, conf, user_id))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error recording prediction: {e}")
        raise
    finally:
        cur.close()

async def process_expert_search(query: str, user_id: str, active_only: bool = True) -> SearchResponse:
    """Process expert search with analytics"""
    conn = None
    try:
        conn = get_db_connection()
        
        # Get or create session
        session_id = await get_or_create_session(conn, user_id)
        
        # Execute search
        start_time = datetime.utcnow()
        search_manager = ExpertSearchIndexManager()
        results = search_manager.search_experts(query, k=5, active_only=active_only)
        response_time = (datetime.utcnow() - start_time).total_seconds()
        
        # Format results
        formatted_results = [
            ExpertSearchResult(
                id=str(result['id']),
                first_name=result['first_name'],
                last_name=result['last_name'],
                designation=result.get('designation', ''),
                theme=result.get('theme', ''),
                unit=result.get('unit', ''),
                contact=result.get('contact', ''),
                is_active=result.get('is_active', True),
                score=result.get('score'),
                bio=result.get('bio'),
                knowledge_expertise=result.get('knowledge_expertise', [])
            ) for result in results
        ]
        
        # Record analytics
        await record_search(conn, session_id, user_id, query, results, response_time)
        
        # Update ML predictor
        try:
            ml_predictor.update(query, user_id=user_id)
        except Exception as e:
            logger.error(f"ML predictor update failed: {e}")
        
        return SearchResponse(
            total_results=len(formatted_results),
            experts=formatted_results,
            user_id=user_id,
            session_id=session_id
        )
        
    except Exception as e:
        logger.error(f"Error searching experts: {e}")
        raise HTTPException(status_code=500, detail="Search processing failed")
    finally:
        if conn:
            conn.close()

async def process_query_prediction(partial_query: str, user_id: str) -> PredictionResponse:
    """Process query prediction request"""
    conn = None
    try:
        conn = get_db_connection()
        session_id = await get_or_create_session(conn, user_id)
        
        predictions = ml_predictor.predict(partial_query, user_id=user_id)
        confidence_scores = [1.0 - (i * 0.1) for i in range(len(predictions))]
        
        await record_prediction(
            conn,
            session_id,
            user_id,
            partial_query,
            predictions,
            confidence_scores
        )
        
        return PredictionResponse(
            predictions=predictions,
            confidence_scores=confidence_scores,
            user_id=user_id
        )
        
    except Exception as e:
        logger.error(f"Error predicting queries: {e}")
        raise HTTPException(status_code=500, detail="Prediction failed")
    finally:
        if conn:
            conn.close()

@router.get("/experts/search/{query}")
async def search_experts(
    query: str,
    request: Request,
    active_only: bool = True,
    user_id: str = Depends(get_user_id)
):
    """Expert search endpoint"""
    return await process_expert_search(query, user_id, active_only)

@router.get("/experts/predict/{partial_query}")
async def predict_query(
    partial_query: str,
    request: Request,
    user_id: str = Depends(get_user_id)
):
    """Get query predictions"""
    return await process_query_prediction(partial_query, user_id)

@router.get("/test/experts/search/{query}")
async def test_search_experts(
    query: str,
    request: Request,
    active_only: bool = True,
    user_id: str = Depends(get_test_user_id)
):
    """Test endpoint for expert search"""
    return await process_expert_search(query, user_id, active_only)

@router.get("/test/experts/predict/{partial_query}")
async def test_predict_query(
    partial_query: str,
    request: Request,
    user_id: str = Depends(get_test_user_id)
):
    """Test endpoint for query prediction"""
    return await process_query_prediction(partial_query, user_id)

@router.get("/experts/analytics")
async def get_analytics(
    request: Request,
    start_date: str,
    end_date: str,
    user_id: str = Depends(get_user_id)
):
    """Get search analytics"""
    conn = None
    try:
        conn = get_db_connection()
        analytics_data = await get_search_analytics(conn, start_date, end_date)
        return analytics_data
    except Exception as e:
        logger.error(f"Error getting analytics: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch analytics")
    finally:
        if conn:
            conn.close()