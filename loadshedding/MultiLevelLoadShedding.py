"""
Multi-level load shedding with circuit breaker pattern for system protection.
"""

import time
from enum import Enum
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from .LoadSheddingStrategy import LoadSheddingStrategy
from .LoadMonitor import LoadLevel
from base.Event import Event


class CircuitBreakerState(Enum):
    """Circuit breaker states for system protection."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # System overloaded, rejecting requests
    HALF_OPEN = "half_open"  # Testing if system has recovered


class SheddingLevel(Enum):
    """Different levels of load shedding intensity."""
    NONE = 0
    LIGHT = 1      # Drop 10-20% of low-priority events
    MODERATE = 2   # Drop 30-50% of events, keep high priority
    HEAVY = 3      # Drop 60-80% of events, only critical events pass
    EXTREME = 4    # Drop 90%+ of events, emergency mode


class MultiLevelLoadShedding(LoadSheddingStrategy):
    """
    Multi-level load shedding strategy with circuit breaker protection.
    
    This strategy implements graduated load shedding levels and includes
    a circuit breaker to protect the system from complete overload.
    """
    
    def __init__(self, 
                 failure_threshold: int = 100,
                 recovery_timeout_seconds: int = 30,
                 half_open_max_calls: int = 10):
        """
        Initialize multi-level load shedding strategy.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout_seconds: Time to wait before trying half-open
            half_open_max_calls: Max calls to test in half-open state
        """
        super().__init__("MultiLevelLoadShedding")
        
        # Circuit breaker parameters
        self.failure_threshold = failure_threshold
        self.recovery_timeout = timedelta(seconds=recovery_timeout_seconds)
        self.half_open_max_calls = half_open_max_calls
        
        # Circuit breaker state
        self.circuit_state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.half_open_calls = 0
        self.half_open_successes = 0
        
        # Load shedding levels
        self.current_shedding_level = SheddingLevel.NONE
        self.level_transition_history = []
        
        # Level-specific drop rates
        self.drop_rates = {
            SheddingLevel.NONE: 0.0,
            SheddingLevel.LIGHT: 0.15,
            SheddingLevel.MODERATE: 0.4,
            SheddingLevel.HEAVY: 0.7,
            SheddingLevel.EXTREME: 0.9
        }
        
        # Load level to shedding level mapping
        self.load_to_shedding_mapping = {
            LoadLevel.NORMAL: SheddingLevel.NONE,
            LoadLevel.MEDIUM: SheddingLevel.LIGHT,
            LoadLevel.HIGH: SheddingLevel.MODERATE,
            LoadLevel.CRITICAL: SheddingLevel.HEAVY
        }
        
        # Event importance classifier
        self.importance_classifier = self._default_importance_classifier
        
        # Performance tracking
        self.level_performance_stats = {level: {
            'events_processed': 0,
            'events_dropped': 0,
            'avg_processing_time': 0.0,
            'system_recovery_time': 0.0
        } for level in SheddingLevel}
    
    def should_drop_event(self, event: Event, load_level: LoadLevel) -> bool:
        """
        Determine whether to drop event using multi-level strategy and circuit breaker.
        
        Args:
            event: Event to evaluate
            load_level: Current system load level
            
        Returns:
            bool: True if event should be dropped
        """
        self.total_events_seen += 1
        
        # Check circuit breaker state first
        if self._check_circuit_breaker():
            self.events_dropped += 1
            return True  # Circuit is open, drop all events
        
        # Update shedding level based on current load
        self._update_shedding_level(load_level)
        
        # Apply load shedding based on current level
        should_drop = self._apply_shedding_level(event)
        
        if should_drop:
            self.events_dropped += 1
            # Record as a failure for circuit breaker if this is a high-importance event
            if self._is_high_importance_event(event):
                self._record_failure()
        else:
            # Record as success for circuit breaker
            self._record_success()
        
        return should_drop
    
    def _check_circuit_breaker(self) -> bool:
        """
        Check and update circuit breaker state.
        
        Returns:
            bool: True if circuit is open and events should be dropped
        """
        current_time = datetime.now()
        
        if self.circuit_state == CircuitBreakerState.CLOSED:
            # Check if we should open the circuit
            if self.failure_count >= self.failure_threshold:
                self.circuit_state = CircuitBreakerState.OPEN
                self.last_failure_time = current_time
                self.current_shedding_level = SheddingLevel.EXTREME
                print(f"Circuit breaker OPENED due to {self.failure_count} failures")
                return True
                
        elif self.circuit_state == CircuitBreakerState.OPEN:
            # Check if we should try half-open
            if (current_time - self.last_failure_time) >= self.recovery_timeout:
                self.circuit_state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
                self.half_open_successes = 0
                print("Circuit breaker moving to HALF_OPEN state")
                return False
            return True  # Still open
            
        elif self.circuit_state == CircuitBreakerState.HALF_OPEN:
            # In half-open state, allow limited calls through
            if self.half_open_calls >= self.half_open_max_calls:
                # Decide whether to close or re-open based on success rate
                success_rate = self.half_open_successes / self.half_open_calls
                if success_rate >= 0.7:  # 70% success rate needed
                    self.circuit_state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    print("Circuit breaker CLOSED - system recovered")
                else:
                    self.circuit_state = CircuitBreakerState.OPEN
                    self.last_failure_time = current_time
                    print("Circuit breaker RE-OPENED - system not yet recovered")
                    return True
        
        return False
    
    def _update_shedding_level(self, load_level: LoadLevel):
        """Update current shedding level based on load level."""
        new_level = self.load_to_shedding_mapping.get(load_level, SheddingLevel.NONE)
        
        # If circuit breaker is open, force extreme shedding
        if self.circuit_state == CircuitBreakerState.OPEN:
            new_level = SheddingLevel.EXTREME
        
        if new_level != self.current_shedding_level:
            # Record transition
            self.level_transition_history.append({
                'timestamp': datetime.now(),
                'from_level': self.current_shedding_level,
                'to_level': new_level,
                'load_level': load_level,
                'circuit_state': self.circuit_state
            })
            
            self.current_shedding_level = new_level
    
    def _apply_shedding_level(self, event: Event) -> bool:
        """
        Apply current shedding level to determine if event should be dropped.
        
        Args:
            event: Event to evaluate
            
        Returns:
            bool: True if event should be dropped
        """
        if self.current_shedding_level == SheddingLevel.NONE:
            return False
        
        # Get event importance
        importance = self.importance_classifier(event)
        
        # Different drop logic based on shedding level
        if self.current_shedding_level == SheddingLevel.LIGHT:
            # Drop only low importance events, with some randomness
            return importance < 0.3 and self._random_drop(0.2)
        
        elif self.current_shedding_level == SheddingLevel.MODERATE:
            # Drop medium and low importance events
            if importance < 0.2:
                return True  # Always drop very low importance
            elif importance < 0.6:
                return self._random_drop(0.5)  # 50% chance for medium importance
            return False  # Keep high importance
        
        elif self.current_shedding_level == SheddingLevel.HEAVY:
            # Only keep high importance events
            if importance < 0.4:
                return True  # Drop low importance
            elif importance < 0.8:
                return self._random_drop(0.7)  # 70% chance to drop medium importance
            return False  # Keep high importance
        
        elif self.current_shedding_level == SheddingLevel.EXTREME:
            # Emergency mode - only keep critical events
            return importance < 0.9  # Only keep events with 90%+ importance
        
        return False
    
    def _random_drop(self, probability: float) -> bool:
        """Helper method for probabilistic dropping."""
        import random
        return random.random() < probability
    
    def _is_high_importance_event(self, event: Event) -> bool:
        """Check if event is high importance (for circuit breaker logic)."""
        return self.importance_classifier(event) > 0.7
    
    def _record_failure(self):
        """Record a failure for circuit breaker."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.circuit_state == CircuitBreakerState.HALF_OPEN:
            self.half_open_calls += 1
    
    def _record_success(self):
        """Record a success for circuit breaker."""
        if self.circuit_state == CircuitBreakerState.HALF_OPEN:
            self.half_open_calls += 1
            self.half_open_successes += 1
        
        # Reset failure count on successful processing (with some decay)
        if self.failure_count > 0:
            self.failure_count = max(0, self.failure_count - 1)
    
    def _default_importance_classifier(self, event: Event) -> float:
        """
        Default classifier for event importance.
        
        Args:
            event: Event to classify
            
        Returns:
            float: Importance score (0.0 to 1.0)
        """
        importance = 0.5  # Base importance
        
        # Check event type
        if hasattr(event, 'type'):
            type_str = str(event.type).lower()
            if any(keyword in type_str for keyword in ['critical', 'error', 'alarm', 'emergency']):
                importance += 0.4
            elif any(keyword in type_str for keyword in ['warning', 'important', 'high']):
                importance += 0.2
            elif any(keyword in type_str for keyword in ['debug', 'trace', 'info']):
                importance -= 0.2
        
        # Check data content for importance indicators
        if hasattr(event, 'data') and event.data:
            data_str = str(event.data).lower()
            if any(keyword in data_str for keyword in ['urgent', 'priority', 'critical', 'immediate']):
                importance += 0.3
        
        # Recent events are more important
        if hasattr(event, 'timestamp'):
            age_seconds = (datetime.now() - event.timestamp).total_seconds()
            if age_seconds < 10:  # Very recent
                importance += 0.1
            elif age_seconds > 300:  # More than 5 minutes old
                importance -= 0.2
        
        return max(0.0, min(1.0, importance))
    
    def get_detailed_statistics(self) -> dict:
        """Get detailed statistics about multi-level load shedding."""
        return {
            'current_shedding_level': self.current_shedding_level.name,
            'circuit_breaker_state': self.circuit_state.name,
            'failure_count': self.failure_count,
            'total_level_transitions': len(self.level_transition_history),
            'recent_transitions': self.level_transition_history[-5:] if self.level_transition_history else [],
            'drop_rate_by_level': {level.name: self.drop_rates[level] for level in SheddingLevel},
            'performance_stats': {level.name: stats for level, stats in self.level_performance_stats.items()},
            'half_open_success_rate': (
                self.half_open_successes / max(1, self.half_open_calls) 
                if self.circuit_state == CircuitBreakerState.HALF_OPEN else None
            )
        }
    
    def set_importance_classifier(self, classifier: Callable[[Event], float]):
        """Set custom importance classifier."""
        self.importance_classifier = classifier
