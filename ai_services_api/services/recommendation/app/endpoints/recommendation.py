# API Router Code
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from typing import List, Dict, Any
from datetime import datetime
import logging
import json
import psycopg2
from ai_services_api.services.recommendation.services.expert_matching import ExpertMatchingService
from ai_services_api.services.message.core.database import get_db_connection

router = APIRouter()
logger = logging.getLogger(__name__)

TEST_USER_ID = "test_user_123"  # Constant for test user

async def record_expert_match(expert_id: str, matched_expert_id: str, similarity_score: float, user_id: str = None, 
                            shared_domains: List[str] = None, shared_fields: int = 0, shared_skills: int = 0):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO expert_matching_logs
                (expert_id, matched_expert_id, similarity_score, shared_domains, 
                shared_fields, shared_skills, user_id, created_at)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (expert_id, matched_expert_id, similarity_score, shared_domains, 
              shared_fields, shared_skills, user_id))
        
        match_id = cur.fetchone()[0]
        conn.commit()
        return match_id
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error recording expert match: {e}")
        return None
    finally:
        if conn:
            conn.close()

@router.post("/recommend/{expert_id}", response_model=Dict)
async def recommend_similar_experts(expert_id: str, user_id: str, request: Request):
    """Production endpoint requiring user_id"""
    try:
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")

        expert_matching = ExpertMatchingService()
        try:
            start_time = datetime.utcnow()
            recommendations = await expert_matching.find_similar_experts(expert_id)
            
            if recommendations:
                for rec in recommendations:
                    await record_expert_match(
                        expert_id=expert_id,
                        matched_expert_id=rec['id'],
                        similarity_score=rec.get('similarity_score', 0.0),
                        user_id=user_id,
                        shared_domains=rec.get('shared_domains', []),
                        shared_fields=rec.get('shared_fields', 0),
                        shared_skills=rec.get('shared_skills', 0)
                    )
            
            return {
                "expert_id": expert_id,
                "user_id": user_id,
                "recommendations": recommendations or [],
                "total_matches": len(recommendations) if recommendations else 0,
                "timestamp": datetime.utcnow(),
                "response_time": (datetime.utcnow() - start_time).total_seconds()
            }
            
        finally:
            expert_matching.close()
            
    except ValueError as ve:
        logger.error(f"Invalid input for expert_id {expert_id}: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except psycopg2.Error as dbe:
        logger.error(f"Database error: {dbe}")
        raise HTTPException(status_code=503, detail="Database connection error")
    except Exception as e:
        logger.error(f"Error processing recommendations: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error processing recommendations")

@router.post("/test/recommend/{expert_id}", response_model=Dict)
async def test_recommend_similar_experts(expert_id: str, request: Request):
    """Test endpoint using TEST_USER_ID"""
    try:
        expert_matching = ExpertMatchingService()
        try:
            start_time = datetime.utcnow()
            recommendations = await expert_matching.find_similar_experts(expert_id)
            
            if recommendations:
                for rec in recommendations:
                    await record_expert_match(
                        expert_id=expert_id,
                        matched_expert_id=rec['id'],
                        similarity_score=rec.get('similarity_score', 0.0),
                        user_id=TEST_USER_ID,
                        shared_domains=rec.get('shared_domains', []),
                        shared_fields=rec.get('shared_fields', 0),
                        shared_skills=rec.get('shared_skills', 0)
                    )
            
            return {
                "expert_id": expert_id,
                "user_id": TEST_USER_ID,
                "recommendations": recommendations or [],
                "total_matches": len(recommendations) if recommendations else 0,
                "timestamp": datetime.utcnow(),
                "response_time": (datetime.utcnow() - start_time).total_seconds()
            }
            
        finally:
            expert_matching.close()
            
    except ValueError as ve:
        logger.error(f"Invalid input for expert_id {expert_id}: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except psycopg2.Error as dbe:
        logger.error(f"Database error: {dbe}")
        raise HTTPException(status_code=503, detail="Database connection error")
    except Exception as e:
        logger.error(f"Error processing recommendations: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error processing recommendations")