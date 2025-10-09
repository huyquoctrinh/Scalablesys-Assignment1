"""
Enhanced Load Shedding Strategy specifically for Hot Path Detection
This module provides specialized load shedding strategies for the CitiBike hot path pattern.
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import random
from base.Event import Event
from base.PatternMatch import PatternMatch
from .LoadSheddingStrategy import LoadSheddingStrategy
from .LoadMonitor import LoadLevel
from .HotPathPartialMatchManager import HotPathPartialMatchManager, BikeChain


class HotPathLoadSheddingStrategy(LoadSheddingStrategy):
    """
    Specialized load shedding strategy for hot path detection patterns.
    
    This strategy considers bike chaining, station popularity, time patterns,
    and partial match completion when making dropping decisions.
    """
    
    def __init__(self, 
                 pattern_priorities: Dict[str, float] = None,
                 importance_attributes: List[str] = None,
                 max_partial_matches: int = 50000,
                 bike_chain_priority_weight: float = 0.6):
        """
        Initialize hot path load shedding strategy.
        
        Args:
            pattern_priorities: Pattern priorities for different hot path types
            importance_attributes: Event attributes indicating importance
            max_partial_matches: Maximum number of partial matches to maintain
            bike_chain_priority_weight: Weight for bike chain priority in decisions
        """
        super().__init__("HotPathLoadSheddingStrategy")
        
        self.pattern_priorities = pattern_priorities or {
            'RushHourHotPath': 10.0,
            'SubscriberHotPath': 8.0,
            'PopularStationHotPath': 7.0,
            'LongDistanceHotPath': 6.0,
            'RegularHotPath': 5.0
        }
        
        self.importance_attributes = importance_attributes or [
            'importance', 'priority', 'time_criticality', 'station_importance'
        ]
        
        self.max_partial_matches = max_partial_matches
        self.bike_chain_priority_weight = bike_chain_priority_weight
        
        # Initialize partial match manager
        self.partial_match_manager = HotPathPartialMatchManager(
            max_chains=max_partial_matches // 10  # Estimate 10 partial matches per chain
        )
        
        # Load-based drop thresholds
        self.load_thresholds = {
            LoadLevel.NORMAL: 1.0,    # Don't drop anything
            LoadLevel.MEDIUM: 0.7,    # Drop events with importance < 0.7
            LoadLevel.HIGH: 0.4,      # Drop events with importance < 0.4
            LoadLevel.CRITICAL: 0.1   # Drop events with importance < 0.1
        }
        
        # Station popularity cache (will be populated from data analysis)
        self.station_popularity = {}
        self.target_stations = {7, 8, 9}
        
        # Time-based importance weights
        self.rush_hour_weight = 1.5
        self.business_hour_weight = 1.2
        self.off_peak_weight = 0.8
        
        # User type weights
        self.subscriber_weight = 1.3
        self.customer_weight = 1.0
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop an event based on hot path specific criteria.
        
        Args:
            event: The event to evaluate
            load_level: Current system load level
            
        Returns:
            bool: True if event should be dropped
        """
        self.total_events_seen += 1
        
        if load_level == LoadLevel.NORMAL:
            return False
        
        # Calculate hot path specific importance
        event_importance = self._calculate_hot_path_importance(event)
        threshold = self.load_thresholds.get(load_level, 0.0)
        
        should_drop = event_importance < threshold
        
        if should_drop:
            self.events_dropped += 1
        
        return should_drop
    
    def should_drop_partial_match(self, partial_match: PatternMatch, 
                                bike_id: str, station_id: str, 
                                load_level: LoadLevel) -> bool:
        """
        Determine whether to drop a partial match based on bike chain analysis.
        
        Args:
            partial_match: The partial match to evaluate
            bike_id: ID of the bike for this trip
            station_id: Station ID for chaining validation
            load_level: Current system load level
            
        Returns:
            bool: True if partial match should be dropped
        """
        return not self.partial_match_manager.add_partial_match(
            partial_match, bike_id, station_id, load_level
        )
    
    def _calculate_hot_path_importance(self, event: Event) -> float:
        """
        Calculate importance score specifically for hot path detection.
        
        Args:
            event: Event to evaluate
            
        Returns:
            float: Importance score between 0.0 and 1.0
        """
        importance_score = 0.5  # Base importance
        
        # Extract event data
        event_data = self._extract_event_data(event)
        if not event_data:
            return importance_score
        
        # Time-based importance
        time_importance = self._calculate_time_importance(event_data)
        importance_score = max(importance_score, time_importance)
        
        # User type importance
        user_importance = self._calculate_user_importance(event_data)
        importance_score = max(importance_score, user_importance)
        
        # Station importance
        station_importance = self._calculate_station_importance(event_data)
        importance_score = max(importance_score, station_importance)
        
        # Trip characteristics importance
        trip_importance = self._calculate_trip_importance(event_data)
        importance_score = max(importance_score, trip_importance)
        
        # Bike chain context importance
        chain_importance = self._calculate_chain_importance(event_data)
        importance_score = max(importance_score, chain_importance)
        
        return min(1.0, max(0.0, importance_score))
    
    def _extract_event_data(self, event: Event) -> Optional[Dict]:
        """Extract event data from various event formats."""
        if hasattr(event, 'payload'):
            return event.payload
        elif hasattr(event, 'get_payload'):
            return event.get_payload()
        elif isinstance(event, dict):
            return event
        else:
            return None
    
    def _calculate_time_importance(self, event_data: Dict) -> float:
        """Calculate importance based on time patterns."""
        timestamp = event_data.get('ts')
        if not timestamp:
            return 0.5
        
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                return 0.5
        
        hour = timestamp.hour
        weekday = timestamp.weekday()
        
        # Rush hour importance
        if weekday < 5:  # Weekday
            if 7 <= hour <= 9:  # Morning rush
                return 0.9 * self.rush_hour_weight
            elif 17 <= hour <= 19:  # Evening rush
                return 0.9 * self.rush_hour_weight
            elif 6 <= hour <= 20:  # Business hours
                return 0.7 * self.business_hour_weight
        else:  # Weekend
            if 10 <= hour <= 18:  # Weekend activity
                return 0.6 * self.business_hour_weight
        
        return 0.4 * self.off_peak_weight
    
    def _calculate_user_importance(self, event_data: Dict) -> float:
        """Calculate importance based on user type."""
        user_type = event_data.get('usertype', '')
        
        if user_type == 'Subscriber':
            return 0.8 * self.subscriber_weight
        elif user_type == 'Customer':
            return 0.6 * self.customer_weight
        
        return 0.5
    
    def _calculate_station_importance(self, event_data: Dict) -> float:
        """Calculate importance based on station characteristics."""
        start_station = event_data.get('start_station_id')
        end_station = event_data.get('end_station_id')
        
        importance = 0.5
        
        # Check if near target stations
        if start_station and str(start_station).isdigit():
            station_id = int(start_station)
            if station_id in self.target_stations:
                importance = max(importance, 0.8)
            elif 5 <= station_id <= 11:  # Near target stations
                importance = max(importance, 0.7)
        
        if end_station and str(end_station).isdigit():
            station_id = int(end_station)
            if station_id in self.target_stations:
                importance = max(importance, 0.9)  # Ending at target is very important
            elif 5 <= station_id <= 11:
                importance = max(importance, 0.6)
        
        # Station popularity
        if start_station in self.station_popularity:
            popularity = self.station_popularity[start_station]
            importance = max(importance, 0.5 + popularity * 0.3)
        
        return min(1.0, importance)
    
    def _calculate_trip_importance(self, event_data: Dict) -> float:
        """Calculate importance based on trip characteristics."""
        duration = event_data.get('tripduration_s', 0)
        if not duration:
            return 0.5
        
        # Trip duration importance
        if duration > 3600:  # Very long trips (>1 hour)
            return 0.8
        elif duration > 1800:  # Long trips (>30 minutes)
            return 0.7
        elif duration > 900:  # Medium trips (>15 minutes)
            return 0.6
        elif duration < 300:  # Very short trips (<5 minutes)
            return 0.3
        
        return 0.5
    
    def _calculate_chain_importance(self, event_data: Dict) -> float:
        """Calculate importance based on bike chain context."""
        bike_id = event_data.get('bikeid')
        if not bike_id:
            return 0.5
        
        # Check if this bike has an active chain
        if bike_id in self.partial_match_manager.active_chains:
            chain = self.partial_match_manager.active_chains[bike_id]
            return chain.get_priority_score()
        
        # For new chains, estimate importance
        return self._estimate_new_chain_importance(event_data)
    
    def _estimate_new_chain_importance(self, event_data: Dict) -> float:
        """Estimate importance for a new bike chain."""
        importance = 0.5
        
        # Time-based estimation
        timestamp = event_data.get('ts')
        if timestamp:
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    pass
            
            if hasattr(timestamp, 'hour'):
                hour = timestamp.hour
                if 7 <= hour <= 9 or 17 <= hour <= 19:
                    importance += 0.2
        
        # User type estimation
        if event_data.get('usertype') == 'Subscriber':
            importance += 0.1
        
        # Station estimation
        start_station = event_data.get('start_station_id')
        if start_station and str(start_station).isdigit():
            station_id = int(start_station)
            if station_id in self.target_stations:
                importance += 0.2
            elif 5 <= station_id <= 11:
                importance += 0.1
        
        return min(1.0, importance)
    
    def update_station_popularity(self, station_popularity: Dict[str, float]):
        """Update station popularity data for better importance calculation."""
        self.station_popularity = station_popularity
    
    def get_completion_candidates(self) -> List[Tuple[str, BikeChain]]:
        """Get bike chains that are close to completion."""
        return self.partial_match_manager.get_completion_candidates()
    
    def get_partial_match_statistics(self) -> Dict[str, any]:
        """Get statistics about partial match management."""
        return self.partial_match_manager.get_statistics()
    
    def update_load_thresholds(self, new_thresholds: Dict[LoadLevel, float]):
        """Update load shedding thresholds."""
        self.load_thresholds.update(new_thresholds)
        self.partial_match_manager.load_thresholds.update(new_thresholds)
    
    def reset_statistics(self):
        """Reset all statistics."""
        super().reset_statistics()
        self.partial_match_manager.reset_statistics()


class LatencyAwareHotPathLoadShedding(HotPathLoadSheddingStrategy):
    """
    Latency-aware version of hot path load shedding.
    
    This strategy adjusts dropping behavior based on target latency bounds
    (10%, 30%, 50%, 70%, 90% of original latency).
    """
    
    def __init__(self, target_latency_bounds: List[float] = [0.1, 0.3, 0.5, 0.7, 0.9],
                 base_latency_ms: float = 100.0, **kwargs):
        """
        Initialize latency-aware hot path load shedding.
        
        Args:
            target_latency_bounds: List of target latency bounds as fractions
            base_latency_ms: Base latency in milliseconds for calculations
        """
        super().__init__(**kwargs)
        self.name = "LatencyAwareHotPathLoadShedding"
        
        self.target_latency_bounds = target_latency_bounds
        self.base_latency_ms = base_latency_ms
        self.current_latency_ms = base_latency_ms
        self.latency_history = []
        
        # Adaptive thresholds based on latency targets
        self.adaptive_thresholds = {
            LoadLevel.NORMAL: 1.0,
            LoadLevel.MEDIUM: 0.8,
            LoadLevel.HIGH: 0.6,
            LoadLevel.CRITICAL: 0.3
        }
    
    def update_latency_feedback(self, current_latency_ms: float):
        """Update current latency and adjust thresholds accordingly."""
        self.current_latency_ms = current_latency_ms
        self.latency_history.append(current_latency_ms)
        
        # Keep only recent history
        if len(self.latency_history) > 20:
            self.latency_history = self.latency_history[-20:]
        
        # Adjust thresholds based on latency
        self._adjust_thresholds_for_latency()
    
    def _adjust_thresholds_for_latency(self):
        """Adjust dropping thresholds based on current latency."""
        if not self.latency_history:
            return
        
        avg_latency = sum(self.latency_history) / len(self.latency_history)
        latency_ratio = avg_latency / self.base_latency_ms
        
        # Find the appropriate latency bound
        target_bound = 1.0
        for bound in sorted(self.target_latency_bounds, reverse=True):
            if latency_ratio > bound:
                target_bound = bound
                break
        
        # Adjust thresholds based on target bound
        adjustment_factor = 1.0 - (1.0 - target_bound) * 0.5
        
        self.adaptive_thresholds = {
            LoadLevel.NORMAL: 1.0,
            LoadLevel.MEDIUM: max(0.1, 0.8 * adjustment_factor),
            LoadLevel.HIGH: max(0.05, 0.6 * adjustment_factor),
            LoadLevel.CRITICAL: max(0.01, 0.3 * adjustment_factor)
        }
        
        # Update the base thresholds
        self.load_thresholds.update(self.adaptive_thresholds)
        self.partial_match_manager.load_thresholds.update(self.adaptive_thresholds)
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """Override to use adaptive thresholds."""
        self.total_events_seen += 1
        
        if load_level == LoadLevel.NORMAL:
            return False
        
        event_importance = self._calculate_hot_path_importance(event)
        threshold = self.adaptive_thresholds.get(load_level, 0.0)
        
        should_drop = event_importance < threshold
        
        if should_drop:
            self.events_dropped += 1
        
        return should_drop
