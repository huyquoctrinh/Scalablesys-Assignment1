"""Load shedding strategies for CEP pattern matching"""
from datetime import datetime
from typing import List
import logging

logger = logging.getLogger(__name__)

class HotPathLoadShedder:
    """Load shedding strategy for hot path detection"""
    
    def __init__(self, config):
        self.config = config
        self.memory_threshold = config.memory_threshold
        self.max_partial_matches = config.max_partial_matches
        self.shedding_rate = config.shedding_rate
        self.utility_weights = config.utility_weights
        
        self.dropped_partial_matches = 0
        self.total_partial_matches = 0
        
        self.target_stations = {7, 8, 9}
        
        logger.info(f"HotPathLoadShedder initialized with threshold={self.memory_threshold}")
    
    def calculate_utility(self, partial_match, current_time):
        """Calculate utility score for a partial match"""
        utility = 0.0
        
        # Extract events from partial match
        events = self._extract_events(partial_match)
        if not events:
            return 0.0
        
        chain_length = len(events)
        
        # FACTOR 1: Chain length (longer = more valuable)
        utility += chain_length * self.utility_weights.get('chain_length', 3.0)
        
        # FACTOR 2: Last station proximity to target {7,8,9}
        try:
            last_event = events[-1]
            last_payload = self._get_payload(last_event)
            last_station_raw = last_payload.get('end_station_id', 0)
            last_station = int(float(last_station_raw)) if last_station_raw else 0
            
            target_weight = self.utility_weights.get('target_proximity', 10.0)
            if last_station in self.target_stations:
                utility += target_weight
            else:
                distances = [abs(last_station - target) for target in self.target_stations]
                min_distance = min(distances) if distances else 10
                utility += max(0, target_weight * 0.5 - min_distance * 0.5)
        except Exception as e:
            logger.debug(f"Error calculating station proximity: {e}")
        
        # FACTOR 3: Time urgency (closer to window expiration = more urgent)
        try:
            first_event = events[0]
            first_payload = self._get_payload(first_event)
            first_time = first_payload.get('ts')
            
            if isinstance(first_time, datetime):
                elapsed = (current_time - first_time).total_seconds()
                window_seconds = 3600  # 1 hour
                urgency = min(1.0, elapsed / window_seconds)
                utility += urgency * self.utility_weights.get('time_urgency', 4.0)
        except Exception as e:
            logger.debug(f"Error calculating time urgency: {e}")
        
        return utility
    
    def _extract_events(self, partial_match):
        """Extract events from a partial match object"""
        # Handle different partial match structures
        if hasattr(partial_match, 'events'):
            return partial_match.events
        if isinstance(partial_match, (list, tuple)):
            return list(partial_match)
        if isinstance(partial_match, dict) and 'events' in partial_match:
            return partial_match['events']
        return []
    
    def _get_payload(self, event):
        """Extract payload from event"""
        if hasattr(event, 'payload'):
            return event.payload
        if isinstance(event, dict):
            return event
        return {}
    
    def should_shed_load(self, partial_matches):
        """Determine if load shedding should activate"""
        if isinstance(partial_matches, int):
            current_size = partial_matches
        else:
            current_size = len(partial_matches)
        
        threshold_size = int(self.max_partial_matches * self.memory_threshold)
        return current_size >= threshold_size
    
    def shed_partial_matches(self, partial_matches, current_time):
        """Shed low-utility partial matches"""
        if not partial_matches:
            return partial_matches
        
        original_count = len(partial_matches)
        self.total_partial_matches += original_count
        
        # Calculate utility for each
        scored = []
        for pm in partial_matches:
            try:
                utility = self.calculate_utility(pm, current_time)
                scored.append((utility, pm))
            except Exception as e:
                logger.warning(f"Error calculating utility: {e}")
                scored.append((0.0, pm))
        
        # Sort by utility (highest first)
        scored.sort(key=lambda x: x[0], reverse=True)
        
        # Keep top (1 - shedding_rate) percent
        keep_count = int(len(scored) * (1 - self.shedding_rate))
        keep_count = max(1, keep_count)
        
        dropped = len(scored) - keep_count
        self.dropped_partial_matches += dropped
        
        if dropped > 0:
            logger.info(f"SHEDDING: Dropped {dropped}/{original_count} partial matches")
        
        return [pm for utility, pm in scored[:keep_count]]
    
    def get_statistics(self):
        """Get statistics"""
        pm_drop_rate = (self.dropped_partial_matches / self.total_partial_matches 
                       if self.total_partial_matches > 0 else 0)
        
        return {
            'strategy': 'utility_based_hot_path',
            'partial_matches_dropped': self.dropped_partial_matches,
            'total_partial_matches': self.total_partial_matches,
            'drop_rate': pm_drop_rate,
            'memory_threshold': self.memory_threshold,
            'shedding_rate': self.shedding_rate
        }