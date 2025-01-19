from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from typing import List, Dict, Any
from datetime import datetime
import logging
import json
import psycopg2
from ai_services_api.services.recommendation.services.expert_matching import ExpertMatchingService

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/recommend/{expert_id}", response_model=Dict)
async def recommend_similar_experts(expert_id: str, request: Request):
    """Get recommendations for similar experts."""
    try:
        expert_matching = ExpertMatchingService()
        
        try:
            recommendations = await expert_matching.find_similar_experts(expert_id)
            
            if not recommendations:
                return {
                    "expert_id": expert_id,
                    "recommendations": [],
                    "message": "No similar experts found",
                    "timestamp": datetime.utcnow()
                }
            
            return {
                "expert_id": expert_id,
                "recommendations": recommendations,
                "total_matches": len(recommendations),
                "timestamp": datetime.utcnow()
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