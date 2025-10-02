"""
Enhanced load shedding strategies with pattern-aware and adaptive capabilities.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import random
import time
from collections import deque, defaultdict
import numpy as np
from base.Event import Event
from .LoadMonitor import LoadLevel
from .LoadSheddingStrategy import LoadSheddingStrategy


class PatternAwareLoadShedding(LoadSheddingStrategy):
    """
    Load shedding strategy that considers pattern completion probability
    and partial match preservation.
    """
    
    def __init__(self, pattern_priorities: Dict[str, float] = None,
                 completion_probability_threshold: float = 0.3):
        super().__init__("PatternAwareLoadShedding")
        
        self.pattern_priorities = pattern_priorities or {}
        self.completion_probability_threshold = completion_probability_threshold
        
        # Track partial matches and their completion rates
        self.partial_matches = defaultdict(list)
        self.completion_history = defaultdict(deque)
        
        # Load-based thresholds with pattern awareness
        self.load_thresholds = {
            LoadLevel.NORMAL: 0.0,     # Don't drop anything
            LoadLevel.MEDIUM: 0.2,     # Drop low-probability events
            LoadLevel.HIGH: 0.5,       # Drop medium-probability events
            LoadLevel.CRITICAL: 0.8    # Only keep high-probability events
        }
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop event based on pattern completion probability.
        """
        self.total_events_seen += 1
        
        if load_level == LoadLevel.NORMAL:
            return False
        
        # Calculate completion probability for this event
        completion_prob = self._calculate_completion_probability(event)
        threshold = self.load_thresholds.get(load_level, 0.0)
        
        should_drop = completion_prob < threshold
        
        if should_drop:
            self.events_dropped += 1
        else:
            # Track this event for pattern completion analysis
            self._track_event_for_completion(event)
        
        return should_drop
    
    def _calculate_completion_probability(self, event: Event) -> float:
        """
        Calculate the probability that this event will contribute to a pattern match.
        """
        # Base probability
        prob = 0.5
        
        event_data = event.payload if hasattr(event, 'payload') else {}
        
        # Time-based probability (rush hours have higher completion rates)
        hour = event_data.get("ts", None)
        if hour:
            if hasattr(hour, 'hour'):
                hour_val = hour.hour
                if 7 <= hour_val <= 9 or 17 <= hour_val <= 19:
                    prob += 0.3
                elif 6 <= hour_val <= 10 or 16 <= hour_val <= 20:
                    prob += 0.2
        
        # Station-based probability (popular stations have higher completion rates)
        start_station = event_data.get("start_station_id")
        if start_station and start_station in self.completion_history:
            history = self.completion_history[start_station]
            if len(history) > 10:
                recent_completion_rate = sum(history) / len(history)
                prob += 0.2 * recent_completion_rate
        
        # User type probability
        if event_data.get("usertype") == "Subscriber":
            prob += 0.15
        
        # Duration-based probability
        duration = event_data.get("tripduration_s", 0)
        if duration > 600:  # 10+ minutes
            prob += 0.1
        
        return min(1.0, max(0.0, prob))
    
    def _track_event_for_completion(self, event: Event):
        """Track events to learn completion patterns."""
        event_data = event.payload if hasattr(event, 'payload') else {}
        station_id = event_data.get("start_station_id")
        
        if station_id:
            # Store event with timestamp for later completion analysis
            self.partial_matches[station_id].append({
                'event': event,
                'timestamp': time.time(),
                'completed': False
            })
            
            # Clean old entries (older than 1 hour)
            current_time = time.time()
            self.partial_matches[station_id] = [
                pm for pm in self.partial_matches[station_id]
                if current_time - pm['timestamp'] < 3600
            ]


class HybridAdaptiveLoadShedding(LoadSheddingStrategy):
    """
    Hybrid strategy that combines multiple approaches and adapts based on performance.
    """
    
    def __init__(self, strategies: List[LoadSheddingStrategy] = None):
        super().__init__("HybridAdaptiveLoadShedding")
        
        self.strategies = strategies or []
        self.strategy_weights = {i: 1.0 for i in range(len(self.strategies))}
        self.strategy_performance = {i: deque(maxlen=100) for i in range(len(self.strategies))}
        
        # Performance tracking
        self.recent_matches = deque(maxlen=50)
        self.recent_latencies = deque(maxlen=50)
        
        # Adaptation parameters
        self.adaptation_interval = 100  # Adapt every 100 events
        self.events_since_adaptation = 0
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Use weighted combination of strategies to make drop decision.
        """
        self.total_events_seen += 1
        self.events_since_adaptation += 1
        
        if load_level == LoadLevel.NORMAL:
            return False
        
        # Get decisions from all strategies
        decisions = []
        for strategy in self.strategies:
            decision = strategy.should_drop_event(event, load_level)
            decisions.append(1.0 if decision else 0.0)
        
        # Calculate weighted decision
        total_weight = sum(self.strategy_weights.values())
        if total_weight == 0:
            return False
        
        weighted_score = sum(
            decisions[i] * self.strategy_weights[i] 
            for i in range(len(decisions))
        ) / total_weight
        
        # Use threshold-based decision
        threshold = self._get_adaptive_threshold(load_level)
        should_drop = weighted_score > threshold
        
        if should_drop:
            self.events_dropped += 1
        
        # Adapt strategy weights if needed
        if self.events_since_adaptation >= self.adaptation_interval:
            self._adapt_strategy_weights()
            self.events_since_adaptation = 0
        
        return should_drop
    
    def _get_adaptive_threshold(self, load_level: LoadLevel) -> float:
        """Get adaptive threshold based on recent performance."""
        base_thresholds = {
            LoadLevel.NORMAL: 1.0,
            LoadLevel.MEDIUM: 0.6,
            LoadLevel.HIGH: 0.4,
            LoadLevel.CRITICAL: 0.2
        }
        
        base_threshold = base_thresholds.get(load_level, 0.5)
        
        # Adjust based on recent performance
        if len(self.recent_latencies) > 10:
            avg_latency = np.mean(self.recent_latencies)
            if avg_latency > 100:  # High latency
                base_threshold -= 0.1  # Be more aggressive
            elif avg_latency < 50:  # Low latency
                base_threshold += 0.1  # Be more conservative
        
        return max(0.1, min(0.9, base_threshold))
    
    def _adapt_strategy_weights(self):
        """Adapt strategy weights based on recent performance."""
        if len(self.recent_matches) < 10:
            return
        
        # Simple adaptation: increase weights of strategies that preserve more matches
        current_match_rate = len(self.recent_matches) / 50.0
        
        for i, strategy in enumerate(self.strategies):
            strategy_drop_rate = strategy.get_drop_rate()
            
            # Reward strategies with lower drop rates if match rate is good
            if current_match_rate > 0.1:  # Good match rate
                if strategy_drop_rate < 0.3:
                    self.strategy_weights[i] *= 1.1  # Increase weight
            else:  # Poor match rate, need more aggressive dropping
                if strategy_drop_rate > 0.5:
                    self.strategy_weights[i] *= 1.1  # Increase weight
        
        # Normalize weights
        total_weight = sum(self.strategy_weights.values())
        if total_weight > 0:
            for i in self.strategy_weights:
                self.strategy_weights[i] /= total_weight
    
    def update_performance_feedback(self, matches_found: int, latency_ms: float):
        """Update performance feedback for adaptation."""
        self.recent_matches.append(matches_found)
        self.recent_latencies.append(latency_ms)


class GeospatialLoadShedding(LoadSheddingStrategy):
    """
    Load shedding strategy that considers geographic patterns and station importance.
    """
    
    def __init__(self, station_importance_scores: Dict[str, float] = None,
                 geographic_zones: Dict[str, str] = None):
        super().__init__("GeospatialLoadShedding")
        
        self.station_importance_scores = station_importance_scores or {}
        self.geographic_zones = geographic_zones or {}
        
        # Zone-based dropping probabilities
        self.zone_drop_probabilities = {
            'business': {
                LoadLevel.NORMAL: 0.0,
                LoadLevel.MEDIUM: 0.05,
                LoadLevel.HIGH: 0.15,
                LoadLevel.CRITICAL: 0.4
            },
            'residential': {
                LoadLevel.NORMAL: 0.0,
                LoadLevel.MEDIUM: 0.1,
                LoadLevel.HIGH: 0.3,
                LoadLevel.CRITICAL: 0.6
            },
            'tourist': {
                LoadLevel.NORMAL: 0.0,
                LoadLevel.MEDIUM: 0.15,
                LoadLevel.HIGH: 0.4,
                LoadLevel.CRITICAL: 0.7
            },
            'default': {
                LoadLevel.NORMAL: 0.0,
                LoadLevel.MEDIUM: 0.1,
                LoadLevel.HIGH: 0.3,
                LoadLevel.CRITICAL: 0.6
            }
        }
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop event based on geographic importance.
        """
        self.total_events_seen += 1
        
        if load_level == LoadLevel.NORMAL:
            return False
        
        event_data = event.payload if hasattr(event, 'payload') else {}
        
        # Get station importance
        start_station = event_data.get("start_station_id")
        station_importance = self.station_importance_scores.get(start_station, 0.5)
        
        # Get zone-based drop probability
        zone = self.geographic_zones.get(start_station, 'default')
        base_drop_prob = self.zone_drop_probabilities[zone].get(load_level, 0.3)
        
        # Adjust drop probability based on station importance
        adjusted_drop_prob = base_drop_prob * (1.0 - station_importance)
        
        should_drop = random.random() < adjusted_drop_prob
        
        if should_drop:
            self.events_dropped += 1
        
        return should_drop
