from fastapi import APIRouter, HTTPException, BackgroundTasks, Request, Depends
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import logging
from datetime import datetime
from ai_services_api.services.search.indexing.index_creator import ExpertSearchIndexManager
from ai_services_api.services.search.ml.ml_predictor import MLPredictor
from ai_services_api.services.search.db.database_manager import DatabaseManager

router = APIRouter()
logger = logging.getLogger(__name__)

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

class PredictionResponse(BaseModel):
    predictions: List[str]
    confidence_scores: List[float]
    user_id: str

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

async def process_expert_search(
    query: str,
    user_id: str,
    active_only: bool = True
) -> SearchResponse:
    """Simplified expert search processing logic without analytics"""
    try:
        search_manager = ExpertSearchIndexManager()
        
        # Perform search
        results = search_manager.search_experts(query, k=5, active_only=active_only)
        
        formatted_results = [
            ExpertSearchResult(
                id=str(result['id']),
                first_name=result['first_name'],
                last_name=result['last_name'],
                designation=result['designation'],
                theme=result['theme'],
                unit=result['unit'],
                contact=result['contact'],
                is_active=result['is_active'],
                score=result.get('score')
            )
            for result in results
        ]
        
        # Update ML predictor
        ml_predictor.update(query, user_id=user_id)
        
        return SearchResponse(
            total_results=len(formatted_results),
            experts=formatted_results,
            user_id=user_id
        )
        
    except Exception as e:
        logger.error(f"Error searching experts: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while searching experts"
        )

async def process_query_prediction(
    partial_query: str,
    user_id: str
) -> PredictionResponse:
    """Simplified query prediction processing logic without analytics"""
    try:
        # Get predictions from ML model
        predictions = ml_predictor.predict(partial_query, user_id=user_id)
        confidence_scores = [1.0 - (i * 0.1) for i in range(len(predictions))]
        
        return PredictionResponse(
            predictions=predictions,
            confidence_scores=confidence_scores,
            user_id=user_id
        )
        
    except Exception as e:
        logger.error(f"Error predicting queries: {e}")
        raise HTTPException(status_code=500, detail=str(e))
# Test endpoints
@router.get("/test/experts/search/{query}")
async def test_search_experts(
    query: str,
    request: Request,
    active_only: bool = True,
    user_id: str = Depends(get_test_user_id)
):
    """Test endpoint for expert search with analytics tracking"""
    return await process_expert_search(query, user_id, active_only)

@router.get("/test/experts/predict/{partial_query}")
async def test_predict_query(
    partial_query: str,
    request: Request,
    user_id: str = Depends(get_test_user_id)
):
    """Test endpoint for query prediction with analytics tracking"""
    return await process_query_prediction(partial_query, user_id)

# Production endpoints
@router.get("/experts/search/{query}")
async def search_experts(
    query: str,
    request: Request,
    active_only: bool = True,
    user_id: str = Depends(get_user_id)
):
    """Production endpoint for expert search with analytics tracking"""
    return await process_expert_search(query, user_id, active_only)

@router.get("/experts/predict/{partial_query}")
async def predict_query(
    partial_query: str,
    request: Request,
    user_id: str = Depends(get_user_id)
):
    """Production endpoint for query prediction with analytics tracking"""
    return await process_query_prediction(partial_query, user_id)
