"""
Hot Path Partial Match Manager for CitiBike Load Shedding
This module manages partial matches for the hot path detection pattern with bike chaining.
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, deque
import heapq
from base.Event import Event
from base.PatternMatch import PatternMatch
from .LoadMonitor import LoadLevel


class BikeChain:
    """Represents a bike chain (sequence of trips by the same bike)."""
    
    def __init__(self, bike_id: str, start_time: datetime):
        self.bike_id = bike_id
        self.start_time = start_time
        self.last_update = start_time
        self.partial_matches = []  # List of partial matches in this chain
        self.completion_score = 0.0
        self.importance_score = 0.0
        self.station_sequence = []  # Track station sequence for chaining validation
        
    def add_partial_match(self, partial_match: PatternMatch, station_id: str):
        """Add a partial match to this bike chain."""
        self.partial_matches.append(partial_match)
        self.station_sequence.append(station_id)
        self.last_update = partial_match.last_timestamp
        self._update_scores()
    
    def _update_scores(self):
        """Update completion and importance scores."""
        # Completion score based on chain length and proximity to target stations
        self.completion_score = min(1.0, len(self.partial_matches) / 5.0)  # Normalize to 5 trips max
        
        # Check if chain is approaching target stations (7, 8, 9)
        if self.station_sequence:
            last_station = int(self.station_sequence[-1]) if self.station_sequence[-1].isdigit() else 0
            if 6 < last_station < 10:  # Near target stations
                self.completion_score += 0.3
        
        # Importance score based on time patterns and user type
        if self.partial_matches:
            first_event = self.partial_matches[0].events[0] if self.partial_matches[0].events else None
            if first_event and hasattr(first_event, 'payload'):
                payload = first_event.payload
                hour = payload.get('ts', datetime.now()).hour
                
                # Rush hour importance
                if 7 <= hour <= 9 or 17 <= hour <= 19:
                    self.importance_score += 0.4
                
                # User type importance
                if payload.get('usertype') == 'Subscriber':
                    self.importance_score += 0.3
                
                # Trip duration importance
                duration = payload.get('tripduration_s', 0)
                if duration > 1800:  # Long trips
                    self.importance_score += 0.2
    
    def is_expired(self, current_time: datetime, window_hours: int = 1) -> bool:
        """Check if this chain has expired based on time window."""
        return (current_time - self.start_time).total_seconds() > window_hours * 3600
    
    def get_priority_score(self) -> float:
        """Get combined priority score for load shedding decisions."""
        return (self.completion_score * 0.6 + self.importance_score * 0.4)


class HotPathPartialMatchManager:
    """
    Manages partial matches for hot path detection with bike chaining.
    
    This manager tracks bike chains, manages partial matches, and provides
    load shedding decisions based on chain completion and importance.
    """
    
    def __init__(self, max_chains: int = 10000, cleanup_interval_sec: int = 30):
        """
        Initialize the hot path partial match manager.
        
        Args:
            max_chains: Maximum number of active bike chains
            cleanup_interval_sec: Interval for cleaning up expired chains
        """
        self.max_chains = max_chains
        self.cleanup_interval_sec = cleanup_interval_sec
        
        # Active bike chains: bike_id -> BikeChain
        self.active_chains: Dict[str, BikeChain] = {}
        
        # Priority queue for load shedding (min-heap by priority score)
        self.priority_queue = []
        self.chain_priorities = {}  # bike_id -> priority_score
        
        # Statistics
        self.total_chains_created = 0
        self.total_chains_completed = 0
        self.total_chains_dropped = 0
        self.last_cleanup = datetime.now()
        
        # Load shedding thresholds
        self.load_thresholds = {
            LoadLevel.NORMAL: 1.0,    # Don't drop any chains
            LoadLevel.MEDIUM: 0.7,    # Drop chains with priority < 0.7
            LoadLevel.HIGH: 0.4,      # Drop chains with priority < 0.4
            LoadLevel.CRITICAL: 0.1   # Drop chains with priority < 0.1
        }
    
    def add_partial_match(self, partial_match: PatternMatch, bike_id: str, 
                         station_id: str, load_level: LoadLevel) -> bool:
        """
        Add a partial match to the appropriate bike chain.
        
        Args:
            partial_match: The partial match to add
            bike_id: ID of the bike for this trip
            station_id: Station ID for chaining validation
            load_level: Current system load level
            
        Returns:
            bool: True if partial match was added, False if dropped
        """
        current_time = datetime.now()
        
        # Cleanup expired chains periodically
        if (current_time - self.last_cleanup).total_seconds() > self.cleanup_interval_sec:
            self._cleanup_expired_chains(current_time)
            self.last_cleanup = current_time
        
        # Check if we should drop this partial match due to load
        if self._should_drop_partial_match(partial_match, bike_id, load_level):
            self.total_chains_dropped += 1
            return False
        
        # Get or create bike chain
        if bike_id not in self.active_chains:
            if len(self.active_chains) >= self.max_chains:
                # Drop lowest priority chain to make room
                self._drop_lowest_priority_chain()
            
            self.active_chains[bike_id] = BikeChain(bike_id, current_time)
            self.total_chains_created += 1
        
        # Add partial match to chain
        chain = self.active_chains[bike_id]
        chain.add_partial_match(partial_match, station_id)
        
        # Update priority queue
        self._update_chain_priority(bike_id, chain.get_priority_score())
        
        return True
    
    def _should_drop_partial_match(self, partial_match: PatternMatch, 
                                 bike_id: str, load_level: LoadLevel) -> bool:
        """Determine if a partial match should be dropped based on load level."""
        if load_level == LoadLevel.NORMAL:
            return False
        
        # If bike chain exists, check its priority
        if bike_id in self.active_chains:
            chain = self.active_chains[bike_id]
            priority_score = chain.get_priority_score()
            threshold = self.load_thresholds.get(load_level, 0.0)
            return priority_score < threshold
        
        # For new chains, use a more conservative approach
        # Check if we're at capacity and this would be a low-priority chain
        if len(self.active_chains) >= self.max_chains * 0.8:  # 80% capacity
            # Estimate priority for new chain based on event attributes
            estimated_priority = self._estimate_new_chain_priority(partial_match)
            threshold = self.load_thresholds.get(load_level, 0.0)
            return estimated_priority < threshold
        
        return False
    
    def _estimate_new_chain_priority(self, partial_match: PatternMatch) -> float:
        """Estimate priority for a new bike chain based on partial match."""
        if not partial_match.events:
            return 0.5
        
        event = partial_match.events[0]
        if not hasattr(event, 'payload'):
            return 0.5
        
        payload = event.payload
        priority = 0.5  # Base priority
        
        # Time-based priority
        hour = payload.get('ts', datetime.now()).hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hours
            priority += 0.3
        
        # User type priority
        if payload.get('usertype') == 'Subscriber':
            priority += 0.2
        
        # Station priority (if near target stations)
        station_id = payload.get('start_station_id', '0')
        if station_id.isdigit() and 6 < int(station_id) < 10:
            priority += 0.2
        
        return min(1.0, priority)
    
    def _update_chain_priority(self, bike_id: str, priority_score: float):
        """Update the priority of a bike chain in the priority queue."""
        self.chain_priorities[bike_id] = priority_score
        
        # Update or add to priority queue
        # Remove old entry if exists
        self.priority_queue = [(score, bid) for score, bid in self.priority_queue if bid != bike_id]
        
        # Add new entry
        heapq.heappush(self.priority_queue, (priority_score, bike_id))
    
    def _drop_lowest_priority_chain(self):
        """Drop the lowest priority bike chain to make room for new ones."""
        if not self.priority_queue:
            return
        
        # Get lowest priority chain
        while self.priority_queue:
            priority_score, bike_id = heapq.heappop(self.priority_queue)
            if bike_id in self.active_chains:
                del self.active_chains[bike_id]
                if bike_id in self.chain_priorities:
                    del self.chain_priorities[bike_id]
                self.total_chains_dropped += 1
                break
    
    def _cleanup_expired_chains(self, current_time: datetime):
        """Remove expired bike chains."""
        expired_bikes = []
        
        for bike_id, chain in self.active_chains.items():
            if chain.is_expired(current_time):
                expired_bikes.append(bike_id)
        
        for bike_id in expired_bikes:
            del self.active_chains[bike_id]
            if bike_id in self.chain_priorities:
                del self.chain_priorities[bike_id]
        
        # Clean up priority queue
        self.priority_queue = [(score, bid) for score, bid in self.priority_queue 
                              if bid in self.active_chains]
        heapq.heapify(self.priority_queue)
    
    def get_completion_candidates(self, target_stations: List[int] = [7, 8, 9]) -> List[Tuple[str, BikeChain]]:
        """
        Get bike chains that are close to completion (near target stations).
        
        Args:
            target_stations: List of target station IDs
            
        Returns:
            List of tuples (bike_id, chain) for chains near completion
        """
        candidates = []
        
        for bike_id, chain in self.active_chains.items():
            if not chain.station_sequence:
                continue
            
            last_station = chain.station_sequence[-1]
            if last_station.isdigit() and int(last_station) in target_stations:
                candidates.append((bike_id, chain))
        
        # Sort by completion score (highest first)
        candidates.sort(key=lambda x: x[1].completion_score, reverse=True)
        return candidates
    
    def get_statistics(self) -> Dict[str, any]:
        """Get statistics about the partial match manager."""
        return {
            'active_chains': len(self.active_chains),
            'max_chains': self.max_chains,
            'total_chains_created': self.total_chains_created,
            'total_chains_completed': self.total_chains_completed,
            'total_chains_dropped': self.total_chains_dropped,
            'completion_rate': (self.total_chains_completed / 
                              max(1, self.total_chains_created + self.total_chains_dropped)),
            'average_chain_length': sum(len(chain.partial_matches) for chain in self.active_chains.values()) 
                                  / max(1, len(self.active_chains)),
            'priority_distribution': {
                'high': len([p for p in self.chain_priorities.values() if p > 0.7]),
                'medium': len([p for p in self.chain_priorities.values() if 0.4 <= p <= 0.7]),
                'low': len([p for p in self.chain_priorities.values() if p < 0.4])
            }
        }
    
    def reset_statistics(self):
        """Reset all statistics."""
        self.total_chains_created = 0
        self.total_chains_completed = 0
        self.total_chains_dropped = 0
