# loadshedding/shedder.py
"""
Load shedding strategies for CEP pattern matching.
Implements utility-based pruning of partial matches.
"""

from datetime import datetime
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class HotPathLoadShedder:
    """
    Load shedding strategy optimized for hot path detection.
    Prioritizes: longer chains + target stations (7,8,9) + time urgency
    """
    
    def __init__(self, config):
        """
        Args:
            config: LoadSheddingConfig instance
        """
        self.config = config
        self.memory_threshold = config.memory_threshold
        self.max_partial_matches = config.max_partial_matches
        self.shedding_rate = config.shedding_rate
        self.utility_weights = config.utility_weights
        
        # Statistics
        self.dropped_partial_matches = 0
        self.total_partial_matches = 0
        self.dropped_events = 0
        self.total_events = 0
        
        # Target stations for hot path detection
        self.target_stations = {7, 8, 9}
        
        logger.info(f"HotPathLoadShedder initialized with threshold={self.memory_threshold}")
    
    def calculate_utility(self, partial_match, current_time):
        """
        Calculate utility score for a partial match.
        Higher utility = higher priority to keep.
        
        Args:
            partial_match: Partial match object/dict
            current_time: Current datetime
            
        Returns:
            float: Utility score (higher is better)
        """
        utility = 0.0
        
        # Extract chain events (a[])
        chain = self._extract_chain(partial_match)
        if not chain:
            return 0.0
        
        chain_length = len(chain)
        
        # FACTOR 1: Chain length (longer chains are closer to completion)
        weight = self.utility_weights.get('chain_length', 3.0)
        utility += chain_length * weight
        
        # FACTOR 2: Last station proximity to target {7,8,9}
        try:
            last_event_payload = self._get_payload(chain[-1])
            last_station = int(last_event_payload.get('end_station_id', 0))
            
            proximity_weight = self.utility_weights.get('target_proximity', 10.0)
            
            if last_station in self.target_stations:
                utility += proximity_weight  # Already at target!
            else:
                # Distance penalty
                distances = [abs(last_station - target) for target in self.target_stations]
                min_distance = min(distances)
                utility += max(0, proximity_weight * 0.5 - min_distance * 0.5)
        except Exception as e:
            logger.debug(f"Error calculating station proximity: {e}")
        
        # FACTOR 3: Time remaining in window (urgency)
        try:
            first_event_payload = self._get_payload(chain[0])
            first_timestamp = first_event_payload.get('ts')
            
            if isinstance(first_timestamp, datetime):
                elapsed = (current_time - first_timestamp).total_seconds()
                window_seconds = 3600  # 1 hour
                time_remaining_ratio = max(0, 1 - (elapsed / window_seconds))
                
                # More urgent as window closes
                urgency_weight = self.utility_weights.get('time_urgency', 4.0)
                urgency = (1 - time_remaining_ratio) * urgency_weight
                utility += urgency
        except Exception as e:
            logger.debug(f"Error calculating time urgency: {e}")
        
        # FACTOR 4: Event importance (from data attributes)
        try:
            importance_weight = self.utility_weights.get('importance', 2.0)
            avg_importance = sum(
                self._get_payload(e).get('importance', 0.5) 
                for e in chain
            ) / chain_length
            utility += avg_importance * importance_weight
        except Exception as e:
            logger.debug(f"Error calculating importance: {e}")
        
        # FACTOR 5: Priority (subscribers > customers)
        try:
            priority_weight = self.utility_weights.get('priority', 1.5)
            avg_priority = sum(
                self._get_payload(e).get('priority', 5.0) 
                for e in chain
            ) / chain_length
            utility += (avg_priority / 10.0) * priority_weight
        except Exception as e:
            logger.debug(f"Error calculating priority: {e}")
        
        return utility
    
    def _extract_chain(self, partial_match):
        """Extract chain of events from partial match"""
        # Try different possible structures
        if isinstance(partial_match, dict):
            # Dict with 'a' key
            if 'a' in partial_match:
                chain = partial_match['a']
                return chain if isinstance(chain, list) else [chain]
            # Dict with 'events' key
            elif 'events' in partial_match:
                return partial_match['events']
        
        # Object with attributes
        if hasattr(partial_match, 'events'):
            return partial_match.events
        if hasattr(partial_match, 'get_events'):
            return partial_match.get_events()
        
        return []
    
    def _get_payload(self, event):
        """Extract payload from event object"""
        if isinstance(event, dict):
            return event
        if hasattr(event, 'payload'):
            return event.payload
        if hasattr(event, 'get_payload'):
            return event.get_payload()
        return {}
    
    def should_shed_load(self, current_partial_matches):
        """
        Determine if load shedding should be activated.
        
        Args:
            current_partial_matches: List or count of current partial matches
            
        Returns:
            bool: True if shedding needed
        """
        if isinstance(current_partial_matches, int):
            current_size = current_partial_matches
        else:
            current_size = len(current_partial_matches)
        
        threshold_size = int(self.max_partial_matches * self.memory_threshold)
        return current_size >= threshold_size
    
    def shed_partial_matches(self, partial_matches, current_time):
        """
        Shed low-utility partial matches.
        
        Args:
            partial_matches: List of current partial matches
            current_time: Current timestamp
        
        Returns:
            List of retained partial matches
        """
        if not partial_matches:
            return partial_matches
        
        original_count = len(partial_matches)
        self.total_partial_matches += original_count
        
        # Calculate utility for each partial match
        scored_matches = []
        for pm in partial_matches:
            try:
                utility = self.calculate_utility(pm, current_time)
                scored_matches.append((utility, pm))
            except Exception as e:
                logger.warning(f"Error calculating utility: {e}")
                # Keep on error
                scored_matches.append((0.0, pm))
        
        # Sort by utility (descending - highest utility first)
        scored_matches.sort(key=lambda x: x[0], reverse=True)
        
        # Determine how many to keep based on shedding rate
        keep_count = int(len(scored_matches) * (1 - self.shedding_rate))
        keep_count = max(1, keep_count)  # Keep at least one
        
        # Track dropped matches
        dropped = len(scored_matches) - keep_count
        self.dropped_partial_matches += dropped
        
        # Log shedding activity
        if dropped > 0:
            logger.info(f"LOAD SHEDDING: Dropped {dropped}/{original_count} "
                       f"partial matches (kept top {keep_count})")
            if scored_matches:
                logger.debug(f"Utility range: {scored_matches[0][0]:.2f} (kept) to "
                           f"{scored_matches[-1][0]:.2f} (dropped)")
        
        # Return high-utility matches
        return [pm for utility, pm in scored_matches[:keep_count]]
    
    def get_statistics(self):
        """Get load shedding statistics"""
        pm_drop_rate = (self.dropped_partial_matches / self.total_partial_matches 
                       if self.total_partial_matches > 0 else 0)
        
        return {
            'strategy': 'utility_based_hot_path',
            'partial_matches_dropped': self.dropped_partial_matches,
            'total_partial_matches': self.total_partial_matches,
            'drop_rate': pm_drop_rate,
            'events_dropped': self.dropped_events,
            'total_events': self.total_events,
            'memory_threshold': self.memory_threshold,
            'shedding_rate': self.shedding_rate
        }