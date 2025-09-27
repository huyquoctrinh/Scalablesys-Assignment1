"""
Load shedding strategies for the OpenCEP system.
This module defines different strategies for dropping events under high load conditions.
"""

from abc import ABC, abstractmethod
from typing import Dict
import random
from base.Event import Event
from .LoadMonitor import LoadLevel


class LoadSheddingStrategy(ABC):
    """
    Abstract base class for load shedding strategies.
    
    Load shedding strategies determine which events should be dropped
    when the system is under high load.
    """
    
    def __init__(self, name: str = "BaseStrategy"):
        self.name = name
        self.total_events_seen = 0
        self.events_dropped = 0
    
    @abstractmethod
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop the given event based on current load level.
        
        Args:
            event: The event to evaluate
            load_level: Current system load level
            
        Returns:
            bool: True if the event should be dropped, False otherwise
        """
        pass
    
    def get_drop_rate(self) -> float:
        """Get current drop rate."""
        if self.total_events_seen == 0:
            return 0.0
        return self.events_dropped / self.total_events_seen
    
    def reset_statistics(self):
        """Reset drop statistics."""
        self.total_events_seen = 0
        self.events_dropped = 0


class ProbabilisticLoadShedding(LoadSheddingStrategy):
    """
    Drops events based on probability that increases with load level.
    
    This strategy uses predetermined drop probabilities for each load level,
    providing predictable behavior under different load conditions.
    """
    
    def __init__(self, drop_probabilities: Dict[LoadLevel, float] = None):
        """
        Initialize probabilistic load shedding strategy.
        
        Args:
            drop_probabilities: Dictionary mapping load levels to drop probabilities
        """
        super().__init__("ProbabilisticLoadShedding")
        
        if drop_probabilities is None:
            self.drop_probabilities = {
                LoadLevel.NORMAL: 0.0,
                LoadLevel.MEDIUM: 0.1,
                LoadLevel.HIGH: 0.3,
                LoadLevel.CRITICAL: 0.6
            }
        else:
            self.drop_probabilities = drop_probabilities
        
        # Validate probabilities
        for level, prob in self.drop_probabilities.items():
            if not 0.0 <= prob <= 1.0:
                raise ValueError(f"Drop probability for {level} must be between 0.0 and 1.0")
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop event based on probabilistic rules.
        
        Args:
            event: The event to evaluate
            load_level: Current system load level
            
        Returns:
            bool: True if event should be dropped
        """
        self.total_events_seen += 1
        
        drop_probability = self.drop_probabilities.get(load_level, 0.0)
        should_drop = random.random() < drop_probability
        
        if should_drop:
            self.events_dropped += 1
        
        return should_drop


class SemanticLoadShedding(LoadSheddingStrategy):
    """
    Drops events based on semantic importance and pattern priorities.
    
    This strategy considers event attributes and pattern priorities to make
    more intelligent dropping decisions.
    """
    
    def __init__(self, pattern_priorities: Dict[str, float] = None,
                 importance_attributes: list = None):
        """
        Initialize semantic load shedding strategy.
        
        Args:
            pattern_priorities: Dictionary mapping pattern names to priority values (higher = more important)
            importance_attributes: List of event attributes that indicate importance
        """
        super().__init__("SemanticLoadShedding")
        
        self.pattern_priorities = pattern_priorities or {}
        self.importance_attributes = importance_attributes or ['importance', 'priority', 'critical']
        
        # Load-based drop thresholds
        self.load_thresholds = {
            LoadLevel.NORMAL: 1.0,    # Don't drop anything
            LoadLevel.MEDIUM: 0.7,    # Drop events with importance < 0.7
            LoadLevel.HIGH: 0.4,      # Drop events with importance < 0.4
            LoadLevel.CRITICAL: 0.1   # Drop events with importance < 0.1
        }
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop event based on semantic importance.
        
        Args:
            event: The event to evaluate
            load_level: Current system load level
            
        Returns:
            bool: True if event should be dropped
        """
        self.total_events_seen += 1
        
        if load_level == LoadLevel.NORMAL:
            return False
        
        # Calculate event importance
        event_importance = self._calculate_event_importance(event)
        threshold = self.load_thresholds.get(load_level, 0.0)
        
        should_drop = event_importance < threshold
        
        if should_drop:
            self.events_dropped += 1
        
        return should_drop
    
    def _calculate_event_importance(self, event: Event) -> float:
        """
        Calculate the importance score of an event.
        
        Args:
            event: Event to evaluate
            
        Returns:
            float: Importance score between 0.0 and 1.0
        """
        importance_score = 0.5  # Default importance
        
        # Check for explicit importance attributes
        event_data = event.payload if hasattr(event, 'payload') else {}
        
        for attr in self.importance_attributes:
            if attr in event_data:
                try:
                    attr_value = float(event_data[attr])
                    importance_score = max(importance_score, attr_value)
                except (ValueError, TypeError):
                    pass
        
        # Consider event type if we have pattern priorities
        if hasattr(event, 'event_type') and event.event_type in self.pattern_priorities:
            type_priority = self.pattern_priorities[event.event_type]
            # Normalize priority to 0-1 range (assuming max priority is 10)
            normalized_priority = min(1.0, type_priority / 10.0)
            importance_score = max(importance_score, normalized_priority)
        
        return min(1.0, max(0.0, importance_score))


class AdaptiveLoadShedding(LoadSheddingStrategy):
    """
    Adaptive load shedding that learns from system behavior.
    
    This strategy adjusts its behavior based on observed system performance
    and feedback from the processing results.
    """
    
    def __init__(self, initial_aggressiveness: float = 0.5):
        """
        Initialize adaptive load shedding strategy.
        
        Args:
            initial_aggressiveness: Initial aggressiveness level (0.0-1.0)
        """
        super().__init__("AdaptiveLoadShedding")
        
        self.aggressiveness = initial_aggressiveness
        self.performance_history = []
        self.adaptation_rate = 0.1
        
        # Track system performance indicators
        self.recent_latencies = []
        self.recent_throughputs = []
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop event based on adaptive rules.
        
        Args:
            event: The event to evaluate
            load_level: Current system load level
            
        Returns:
            bool: True if event should be dropped
        """
        self.total_events_seen += 1
        
        if load_level == LoadLevel.NORMAL:
            return False
        
        # Calculate drop probability based on load level and current aggressiveness
        base_probabilities = {
            LoadLevel.MEDIUM: 0.1,
            LoadLevel.HIGH: 0.4,
            LoadLevel.CRITICAL: 0.7
        }
        
        base_prob = base_probabilities.get(load_level, 0.0)
        adapted_prob = base_prob * self.aggressiveness
        
        should_drop = random.random() < adapted_prob
        
        if should_drop:
            self.events_dropped += 1
        
        return should_drop
    
    def update_performance_feedback(self, latency_ms: float, throughput_events_per_sec: float):
        """
        Update performance feedback to adapt strategy.
        
        Args:
            latency_ms: Recent processing latency
            throughput_events_per_sec: Recent throughput
        """
        self.recent_latencies.append(latency_ms)
        self.recent_throughputs.append(throughput_events_per_sec)
        
        # Keep only recent measurements
        max_history = 20
        if len(self.recent_latencies) > max_history:
            self.recent_latencies = self.recent_latencies[-max_history:]
        if len(self.recent_throughputs) > max_history:
            self.recent_throughputs = self.recent_throughputs[-max_history:]
        
        # Adapt aggressiveness based on performance
        self._adapt_aggressiveness()
    
    def _adapt_aggressiveness(self):
        """Adapt the aggressiveness level based on recent performance."""
        if len(self.recent_latencies) < 5:
            return  # Not enough data
        
        avg_latency = sum(self.recent_latencies) / len(self.recent_latencies)
        avg_throughput = sum(self.recent_throughputs) / len(self.recent_throughputs)
        
        # If latency is high, increase aggressiveness (drop more)
        target_latency = 100.0  # ms
        if avg_latency > target_latency:
            self.aggressiveness = min(1.0, self.aggressiveness + self.adaptation_rate)
        else:
            # If latency is acceptable, decrease aggressiveness (drop less)
            self.aggressiveness = max(0.1, self.aggressiveness - self.adaptation_rate)


class NoLoadShedding(LoadSheddingStrategy):
    """
    Baseline strategy that never drops events.
    Used for comparison and baseline measurements.
    """
    
    def __init__(self):
        super().__init__("NoLoadShedding")
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """Never drop any events."""
        self.total_events_seen += 1
        return False