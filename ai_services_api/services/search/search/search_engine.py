
import faiss
import pickle
import os
import json
import logging
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional, Any, Union
from datetime import datetime, timedelta

# database and analytics
from ai_services_api.services.search.db.database_manager import DatabaseManager
from ai_services_api.services.search.core.cache_manager import CacheManager
from ai_services_api.services.search.ml.embedding_model import EmbeddingModel
from ai_services_api.services.search.ml.ml_predictor import MLPredictor

class SearchEngine:
    def __init__(self):
        try:
            logger.info("Initializing Production SearchEngine...")
            self._init_components()
            self._load_models()
            self.current_session = None
            logger.info("Production SearchEngine initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing SearchEngine: {e}")
            raise

    def _init_components(self):
        try:
            # Initialize core components
            self.db = DatabaseManager()
            self.cache = CacheManager()
            self.embedding_model = EmbeddingModel()
            self.ml_predictor = MLPredictor()
            
            # Initialize query tracking
            self.query_history = self.db.get_recent_queries(limit=1000)
            self.term_frequencies = self.db.get_term_frequencies()
            
            # Set scoring weights
            self.weights = {
                'term_frequency': 0.3,
                'temporal_relevance': 0.2,
                'semantic_similarity': 0.3,
                'user_preference': 0.2
            }
            
            self.temporal_decay_rate = 0.1
            self.max_history_age = 30  # days
            
        except Exception as e:
            logger.error(f"Error initializing components: {e}")
            raise

    def _start_search_session(self, user_id: str):
        """Start a new search session for the user"""
        try:
            query = """
                INSERT INTO search_sessions (user_id)
                VALUES (%s)
                RETURNING id;
            """
            session_id = self.db.execute_query(query, (user_id,), fetch_one=True)[0]
            self.current_session = {
                'id': session_id,
                'user_id': user_id,
                'start_time': datetime.utcnow()
            }
        except Exception as e:
            logger.error(f"Error starting search session: {e}")

    def _update_search_session(self, success: bool = True):
        """Update the current search session metrics"""
        if not self.current_session:
            return

        try:
            query = """
                UPDATE search_sessions 
                SET query_count = query_count + 1,
                    successful_searches = successful_searches + %s
                WHERE id = %s;
            """
            self.db.execute_query(query, (1 if success else 0, self.current_session['id']))
        except Exception as e:
            logger.error(f"Error updating search session: {e}")

    def search(self, query: str, k: int = 5, user_id: Optional[str] = None, 
               search_type: str = 'general', filters: Optional[Dict] = None) -> List[Dict]:
        start_time = datetime.utcnow()
        try:
            logger.info(f"Searching for: {query}")
            
            # Start or update session
            if user_id:
                if not self.current_session or self.current_session['user_id'] != user_id:
                    self._start_search_session(user_id)
            
            # Check cache first
            cache_key = f"search_{query}_{k}_{user_id}"
            cached_result = self.cache.get(cache_key)
            if cached_result:
                self._log_search(query, user_id, start_time, len(cached_result), 
                               search_type, True, filters)
                return cached_result
            
            # Generate query embedding and search
            query_vector = self.embedding_model.get_embedding(query)
            num_candidates = min(k * 2, self.index.ntotal)
            distances, indices = self.index.search(query_vector, num_candidates)
            
            # Process results
            results = self._process_results(indices[0], distances[0])
            
            # Apply personalization
            if user_id:
                results = self._personalize_results(results, user_id)
            
            # Cache results
            final_results = results[:k]
            self.cache.set(cache_key, final_results, expire=300)
            
            # Log search
            self._log_search(query, user_id, start_time, len(final_results), 
                           search_type, True, filters)
            
            # Update session
            self._update_search_session(True)
            
            return final_results
            
        except Exception as e:
            logger.error(f"Error during search: {e}")
            self._log_search(query, user_id, start_time, 0, search_type, False, filters)
            self._update_search_session(False)
            raise

    def _log_search(self, query: str, user_id: Optional[str], 
                   start_time: datetime, result_count: int,
                   search_type: str, success: bool, filters: Optional[Dict]):
        """Log search details to the database"""
        try:
            response_time = datetime.utcnow() - start_time
            
            query = """
                INSERT INTO search_logs 
                (query, user_id, response_time, result_count, search_type, 
                 success_rate, filters)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """
            params = (
                query, user_id, response_time, result_count, search_type,
                1.0 if success else 0.0, json.dumps(filters) if filters else None
            )
            search_id = self.db.execute_query(query, params, fetch_one=True)[0]
            
            # Log performance metrics
            self._update_performance_metrics(response_time)
            
            return search_id
        except Exception as e:
            logger.error(f"Error logging search: {e}")

    def _update_performance_metrics(self, response_time: timedelta):
        """Update search performance metrics"""
        try:
            current_time = datetime.utcnow()
            hour_start = current_time.replace(minute=0, second=0, microsecond=0)
            
            query = """
                INSERT INTO search_performance 
                (timestamp, avg_response_time, cache_hit_rate, error_rate, 
                 total_queries, unique_users)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (DATE_TRUNC('hour', timestamp))
                DO UPDATE SET
                    avg_response_time = (search_performance.avg_response_time * 
                                       search_performance.total_queries + %s) / 
                                      (search_performance.total_queries + 1),
                    total_queries = search_performance.total_queries + 1;
            """
            
            cache_hit_rate = self.cache.get_hit_rate()
            error_rate = self._calculate_error_rate()
            
            params = (
                hour_start, response_time, cache_hit_rate, error_rate, 
                1, 1, response_time
            )
            self.db.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error updating performance metrics: {e}")

    def predict_queries(self, partial_query: str, limit: int = 5, 
                       user_id: Optional[str] = None) -> List[str]:
        try:
            predictions = super().predict_queries(partial_query, limit, 
                                               {'user_id': user_id})
            
            # Log predictions
            self._log_predictions(partial_query, predictions, user_id)
            
            return predictions
        except Exception as e:
            logger.error(f"Error in predict_queries: {e}")
            return []

    def _log_predictions(self, partial_query: str, predictions: List[str], 
                        user_id: Optional[str]):
        """Log query predictions"""
        try:
            query = """
                INSERT INTO query_predictions 
                (partial_query, predicted_query, confidence_score, user_id)
                VALUES (%s, %s, %s, %s);
            """
            
            for pred in predictions:
                confidence = self._calculate_prediction_score(
                    pred, partial_query, user_id, 
                    self.embedding_model.get_embedding(partial_query)
                )
                params = (partial_query, pred, confidence, user_id)
                self.db.execute_query(query, params)
                
        except Exception as e:
            logger.error(f"Error logging predictions: {e}")

    def log_click(self, search_id: int, result_index: int, 
                  expert_id: Optional[str] = None):
        """Log when a user clicks on a search result"""
        try:
            # Update search_logs
            query = """
                UPDATE search_logs 
                SET clicked = TRUE 
                WHERE id = %s;
            """
            self.db.execute_query(query, (search_id,))
            
            # If expert result, log to expert_searches
            if expert_id:
                query = """
                    INSERT INTO expert_searches 
                    (search_id, expert_id, rank_position, clicked, click_timestamp)
                    VALUES (%s, %s, %s, TRUE, CURRENT_TIMESTAMP);
                """
                self.db.execute_query(query, (search_id, expert_id, result_index))
                
        except Exception as e:
            logger.error(f"Error logging click: {e}")

    def _calculate_error_rate(self) -> float:
        """Calculate current error rate from recent searches"""
        try:
            query = """
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN success_rate = 0 THEN 1 ELSE 0 END) as errors
                FROM search_logs
                WHERE timestamp > NOW() - INTERVAL '1 hour';
            """
            result = self.db.execute_query(query, fetch_one=True)
            total, errors = result
            return errors / total if total > 0 else 0.0
        except Exception as e:
            logger.error(f"Error calculating error rate: {e}")
            return 0.0

    def _load_models(self):
        try:
            current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
            models_dir = current_dir.parent / 'models'
            
            # Load FAISS index
            self.index_path = models_dir / 'faiss_index.idx'
            if not os.path.isfile(self.index_path):
                raise FileNotFoundError(f"FAISS index not found at {self.index_path}")
            self.index = faiss.read_index(str(self.index_path))
            
            # Load document mappings
            self.mapping_path = models_dir / 'chunk_mapping.pkl'
            if not os.path.isfile(self.mapping_path):
                raise FileNotFoundError(f"Chunk mapping not found at {self.mapping_path}")
            with open(self.mapping_path, 'rb') as f:
                self.chunk_mapping = pickle.load(f)
                
        except Exception as e:
            logger.error(f"Error loading models: {e}")
            raise

    def _calculate_prediction_score(self, 
                                 prediction: str, 
                                 partial_query: str,
                                 user_id: Optional[str],
                                 query_embedding: np.ndarray) -> float:
        try:
            # Term frequency score
            term_freq_score = sum(self.term_frequencies.get(term, 0) 
                                for term in prediction.lower().split())
            term_freq_score = min(term_freq_score / 100, 1.0)

            # Temporal relevance
            temporal_score = self._calculate_temporal_relevance(prediction)

            # Semantic similarity
            pred_embedding = self.embedding_model.get_embedding(prediction)
            semantic_score = float(np.dot(query_embedding, pred_embedding.T))
            semantic_score = (semantic_score + 1) / 2

            # User preference score
            user_score = 0.0
            if user_id:
                user_score = self._calculate_user_preference_score(prediction, user_id)

            # Combine scores
            final_score = (
                self.weights['term_frequency'] * term_freq_score +
                self.weights['temporal_relevance'] * temporal_score +
                self.weights['semantic_similarity'] * semantic_score +
                self.weights['user_preference'] * user_score
            )

            return final_score

        except Exception as e:
            logger.error(f"Error calculating prediction score: {e}")
            return 0.0

    def _calculate_temporal_relevance(self, query: str) -> float:
        try:
            recent_queries = self.db.get_query_timestamps(query, limit=10)
            if not recent_queries:
                return 0.0

            current_time = datetime.utcnow()
            scores = []
            
            for timestamp in recent_queries:
                age = (current_time - timestamp).total_seconds()
                max_age = self.max_history_age * 24 * 3600
                if age <= max_age:
                    scores.append(np.exp(-self.temporal_decay_rate * age / max_age))

            return max(scores) if scores else 0.0

        except Exception as e:
            logger.error(f"Error calculating temporal relevance: {e}")
            return 0.0

    def _process_results(self, indices: np.ndarray, distances: np.ndarray) -> List[Dict]:
        results = []
        for idx, (distance, doc_idx) in enumerate(zip(distances, indices)):
            if doc_idx < 0 or doc_idx >= len(self.chunk_mapping):
                continue

            try:
                metadata = self.chunk_mapping[doc_idx]
                max_distance = 100
                similarity = max(0, 1 - (distance / max_distance))
                
                result = {
                    'metadata': metadata,
                    'similarity_score': float(similarity)
                }
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing result {idx}: {e}")
                continue

        results.sort(key=lambda x: x['similarity_score'], reverse=True)
        return results

    def _personalize_results(self, results: List[Dict], user_id: str) -> List[Dict]:
        try:
            user_prefs = self.db.get_user_preferences(user_id)
            if not user_prefs:
                return results

            for result in results:
                if 'tags' in result['metadata']:
                    tags = set(t.strip() for t in result['metadata']['tags'].split('|'))
                    pref_boost = sum(user_prefs.get(tag, 0) for tag in tags) / len(tags)
                    result['similarity_score'] *= (1 + pref_boost)

            results.sort(key=lambda x: x['similarity_score'], reverse=True)
            return results
        except Exception as e:
            logger.error(f"Error in personalization: {e}")

    def _update_query_history(self, query: str, user_id: Optional[str] = None):
        try:
            # Update database
            self.db.add_query(query, user_id)
            
            # Update local cache
            self.query_history.append({
                'query': query,
                'timestamp': datetime.utcnow(),
                'user_id': user_id
            })
            
            # Update term frequencies
            terms = query.lower().split()
            for term in terms:
                self.term_frequencies[term] = self.term_frequencies.get(term, 0) + 1
                
        except Exception as e:
            logger.error(f"Error updating query history: {e}")

    def _get_user_predictions(self, user_id: str, partial_query: str) -> List[str]:
        try:
            user_queries = self.db.get_user_queries(user_id, limit=100)
            return [q for q in user_queries if q.lower().startswith(partial_query.lower())]
        except Exception as e:
            logger.error(f"Error getting user predictions: {e}")
            return []

    def update_user_preferences(self, user_id: str, relevant_docs: List[str]):
        try:
            self.db.update_user_preferences(user_id, relevant_docs)
            logger.info(f"Updated preferences for user {user_id}")
        except Exception as e:
