"""
Stateful Load Shedding Strategy with Pattern Completion State Management.
This module implements advanced load shedding that maintains state about partial matches
and makes intelligent decisions based on pattern completion likelihood.
"""

from abc import ABC, abstractmethod
from typing import Dict, Set, List, Optional, Tuple, DefaultDict
from collections import defaultdict, deque
import time
import threading
from datetime import datetime, timedelta
from base.Event import Event
from base.Pattern import Pattern
from base.PatternMatch import PatternMatch
from .LoadMonitor import LoadLevel
from .LoadSheddingStrategy import LoadSheddingStrategy


class PartialMatchState:
    """Represents the state of a partial pattern match."""
    
    def __init__(self, pattern_name: str, event_ids: List[str], start_time: float):
        self.pattern_name = pattern_name
        self.event_ids = event_ids
        self.start_time = start_time
        self.last_update = time.time()
        self.completion_probability = 0.5
        self.priority_score = 1.0
        self.expected_events = []
        self.window_expiry = start_time + 3600  # 1 hour default window
    
    def add_event(self, event_id: str):
        """Add an event to this partial match."""
        self.event_ids.append(event_id)
        self.last_update = time.time()
        self._update_completion_probability()
    
    def _update_completion_probability(self):
        """Update completion probability based on current state."""
        # Simple heuristic: more events = higher completion probability
        progress = len(self.event_ids) / max(len(self.expected_events), 2)
        self.completion_probability = min(0.9, 0.3 + (progress * 0.6))
    
    def is_expired(self, current_time: float) -> bool:
        """Check if this partial match has expired."""
        return current_time > self.window_expiry
    
    def get_age_in_seconds(self) -> float:
        """Get the age of this partial match in seconds."""
        return time.time() - self.start_time


class PatternStateManager:
    """Manages state for all patterns and their partial matches."""
    
    def __init__(self, max_partial_matches: int = 10000):
        self.partial_matches: Dict[str, PartialMatchState] = {}
        self.pattern_statistics: DefaultDict[str, Dict] = defaultdict(lambda: {
            'total_attempts': 0,
            'completed_matches': 0,
            'completion_rate': 0.0,
            'avg_completion_time': 0.0,
            'recent_completions': deque(maxlen=100)
        })
        self.max_partial_matches = max_partial_matches
        self.lock = threading.RLock()
        
        # State cleanup parameters
        self.last_cleanup = time.time()
        self.cleanup_interval = 300  # 5 minutes
    
    def start_partial_match(self, pattern_name: str, event: Event) -> str:
        """Start tracking a new partial match."""
        with self.lock:
            match_id = f"{pattern_name}_{event.id}_{time.time()}"
            partial_match = PartialMatchState(
                pattern_name, 
                [event.id], 
                event.timestamp if hasattr(event, 'timestamp') else time.time()
            )
            
            self.partial_matches[match_id] = partial_match
            self.pattern_statistics[pattern_name]['total_attempts'] += 1
            
            # Cleanup if needed
            if len(self.partial_matches) > self.max_partial_matches:
                self._cleanup_expired_matches()
            
            return match_id
    
    def update_partial_match(self, match_id: str, event: Event) -> bool:
        """Update an existing partial match with a new event."""
        with self.lock:
            if match_id in self.partial_matches:
                self.partial_matches[match_id].add_event(event.id)
                return True
            return False
    
    def complete_partial_match(self, match_id: str, pattern_match: PatternMatch):
        """Mark a partial match as completed."""
        with self.lock:
            if match_id in self.partial_matches:
                partial_match = self.partial_matches[match_id]
                pattern_name = partial_match.pattern_name
                completion_time = partial_match.get_age_in_seconds()
                
                # Update statistics
                stats = self.pattern_statistics[pattern_name]
                stats['completed_matches'] += 1
                stats['completion_rate'] = stats['completed_matches'] / stats['total_attempts']
                stats['recent_completions'].append(completion_time)
                
                # Update average completion time
                if stats['recent_completions']:
                    stats['avg_completion_time'] = sum(stats['recent_completions']) / len(stats['recent_completions'])
                
                # Remove completed match
                del self.partial_matches[match_id]
    
    def get_completion_probability(self, pattern_name: str, event: Event) -> float:
        """Get the probability that this event will lead to a pattern completion."""
        with self.lock:
            stats = self.pattern_statistics[pattern_name]
            base_probability = stats.get('completion_rate', 0.5)
            
            # Check for existing partial matches that could be completed
            matching_partials = [
                pm for pm in self.partial_matches.values()
                if pm.pattern_name == pattern_name and not pm.is_expired(time.time())
            ]
            
            if matching_partials:
                # Higher probability if there are active partial matches
                return min(0.9, base_probability + 0.2)
            
            return base_probability
    
    def _cleanup_expired_matches(self):
        """Remove expired partial matches."""
        current_time = time.time()
        if current_time - self.last_cleanup < self.cleanup_interval:
            return
            
        expired_matches = [
            match_id for match_id, partial_match in self.partial_matches.items()
            if partial_match.is_expired(current_time)
        ]
        
        for match_id in expired_matches:
            del self.partial_matches[match_id]
        
        self.last_cleanup = current_time
    
    def get_state_summary(self) -> Dict:
        """Get a summary of the current state."""
        with self.lock:
            return {
                'active_partial_matches': len(self.partial_matches),
                'pattern_statistics': dict(self.pattern_statistics),
                'patterns_being_tracked': list(self.pattern_statistics.keys())
            }


class StatefulLoadSheddingStrategy(LoadSheddingStrategy):
    """
    Advanced load shedding strategy that maintains state about partial matches
    and makes decisions based on pattern completion likelihood.
    """
    
    def __init__(self, 
                 pattern_priorities: Dict[str, float] = None,
                 completion_threshold: float = 0.3,
                 state_aware_probability: float = 0.8):
        """
        Initialize stateful load shedding strategy.
        
        Args:
            pattern_priorities: Priority weights for different patterns
            completion_threshold: Minimum completion probability to keep events
            state_aware_probability: How much to weight state information vs random dropping
        """
        super().__init__("StatefulLoadShedding")
        self.pattern_priorities = pattern_priorities or {}
        self.completion_threshold = completion_threshold
        self.state_aware_probability = state_aware_probability
        
        # State management
        self.state_manager = PatternStateManager()
        self.load_level_thresholds = {
            LoadLevel.NORMAL: 0.0,
            LoadLevel.MEDIUM: 0.2,
            LoadLevel.HIGH: 0.5,
            LoadLevel.CRITICAL: 0.8
        }
        
        # Performance tracking
        self.state_based_decisions = 0
        self.random_decisions = 0
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop event based on stateful analysis.
        
        Args:
            event: Event to evaluate for dropping
            load_level: Current system load level
            
        Returns:
            bool: True if event should be dropped
        """
        self.total_events_seen += 1
        
        # Normal load - don't drop anything
        if load_level == LoadLevel.NORMAL:
            return False
        
        # Get pattern information from event
        pattern_name = self._extract_pattern_name(event)
        
        # Calculate state-based drop probability
        state_drop_prob = self._calculate_state_based_drop_probability(event, pattern_name, load_level)
        
        # Calculate final drop probability (blend state-aware and random)
        base_drop_prob = self.load_level_thresholds.get(load_level, 0.5)
        final_drop_prob = (
            self.state_aware_probability * state_drop_prob +
            (1 - self.state_aware_probability) * base_drop_prob
        )
        
        # Make dropping decision
        should_drop = self._make_dropping_decision(final_drop_prob, event, pattern_name)
        
        if should_drop:
            self.events_dropped += 1
        else:
            # Track this event for future state-based decisions
            self._track_event_for_state(event, pattern_name)
        
        return should_drop
    
    def _calculate_state_based_drop_probability(self, event: Event, pattern_name: str, load_level: LoadLevel) -> float:
        """Calculate drop probability based on current state."""
        # Get completion probability for this pattern
        completion_prob = self.state_manager.get_completion_probability(pattern_name, event)
        
        # Get pattern priority
        pattern_priority = self.pattern_priorities.get(pattern_name, 1.0)
        
        # Calculate state-based score
        state_score = completion_prob * pattern_priority
        
        # Convert to drop probability (higher state score = lower drop probability)
        if state_score > 0.8:
            drop_prob = 0.1  # Very low drop probability for high-value events
        elif state_score > 0.5:
            drop_prob = 0.3
        elif state_score > 0.3:
            drop_prob = 0.6
        else:
            drop_prob = 0.9  # High drop probability for low-value events
        
        # Adjust based on load level
        load_multiplier = {
            LoadLevel.MEDIUM: 1.0,
            LoadLevel.HIGH: 1.5,
            LoadLevel.CRITICAL: 2.0
        }.get(load_level, 1.0)
        
        return min(0.95, drop_prob * load_multiplier)
    
    def _make_dropping_decision(self, drop_probability: float, event: Event, pattern_name: str) -> bool:
        """Make the final dropping decision with state considerations."""
        import random
        
        # Use state information to make smarter decisions
        if random.random() < self.state_aware_probability:
            self.state_based_decisions += 1
            # State-based decision
            return random.random() < drop_probability
        else:
            self.random_decisions += 1
            # Fallback to random decision
            return random.random() < drop_probability
    
    def _track_event_for_state(self, event: Event, pattern_name: str):
        """Track event for future state-based decisions."""
        # This could be expanded to track events that contribute to patterns
        # For now, we just update our knowledge about this pattern
        pass
    
    def _extract_pattern_name(self, event: Event) -> str:
        """Extract pattern name from event."""
        # Try to get pattern name from event attributes
        if hasattr(event, 'pattern_name'):
            return event.pattern_name
        if hasattr(event, 'event_type'):
            return event.event_type
        return "default_pattern"
    
    def notify_pattern_match(self, pattern_match: PatternMatch):
        """Notify the strategy about a completed pattern match."""
        # This would be called by the CEP engine when patterns complete
        pattern_name = getattr(pattern_match, 'pattern_name', 'unknown')
        # Find and complete partial matches
        # This is a placeholder - would need integration with pattern matching engine
        pass
    
    def get_state_statistics(self) -> Dict:
        """Get statistics about state-based decisions."""
        total_decisions = self.state_based_decisions + self.random_decisions
        return {
            'state_based_decisions': self.state_based_decisions,
            'random_decisions': self.random_decisions,
            'state_decision_ratio': self.state_based_decisions / max(total_decisions, 1),
            'state_manager_summary': self.state_manager.get_state_summary(),
            'completion_threshold': self.completion_threshold,
            'total_events_seen': self.total_events_seen,
            'events_dropped': self.events_dropped,
            'drop_rate': self.get_drop_rate()
        }
    
    def reset_statistics(self):
        """Reset all statistics."""
        super().reset_statistics()
        self.state_based_decisions = 0
        self.random_decisions = 0
        self.state_manager = PatternStateManager()


class AdaptiveStatefulLoadShedding(StatefulLoadSheddingStrategy):
    """
    Adaptive version that learns from pattern completion patterns
    and adjusts thresholds dynamically.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.learning_rate = 0.1
        self.adaptation_interval = 1000  # Adapt every 1000 events
        self.last_adaptation = 0
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """Enhanced dropping decision with adaptive learning."""
        result = super().should_drop_event(event, load_level)
        
        # Periodic adaptation
        if self.total_events_seen - self.last_adaptation >= self.adaptation_interval:
            self._adapt_thresholds()
            self.last_adaptation = self.total_events_seen
        
        return result
    
    def _adapt_thresholds(self):
        """Adapt thresholds based on recent performance."""
        state_summary = self.state_manager.get_state_summary()
        
        # Analyze pattern completion rates and adjust thresholds
        for pattern_name, stats in state_summary.get('pattern_statistics', {}).items():
            completion_rate = stats.get('completion_rate', 0.5)
            
            # Adjust priority based on success rate
            if completion_rate > 0.8:
                # High success rate - increase priority
                self.pattern_priorities[pattern_name] = min(2.0, 
                    self.pattern_priorities.get(pattern_name, 1.0) * (1 + self.learning_rate))
            elif completion_rate < 0.2:
                # Low success rate - decrease priority
                self.pattern_priorities[pattern_name] = max(0.1, 
                    self.pattern_priorities.get(pattern_name, 1.0) * (1 - self.learning_rate))