"""
Priority-based event queue with intelligent buffering for load shedding.
"""

import heapq
import threading
from typing import Optional, List, Tuple, Callable
from datetime import datetime, timedelta
from collections import defaultdict
from base.Event import Event
from .LoadMonitor import LoadLevel


class PriorityEvent:
    """Wrapper class for events with priority."""
    
    def __init__(self, event: Event, priority: float, timestamp: datetime):
        self.event = event
        self.priority = priority
        self.timestamp = timestamp
        self.buffer_entry_time = datetime.now()
        
    def __lt__(self, other):
        # Higher priority events come first (reverse order for min-heap)
        if self.priority != other.priority:
            return self.priority > other.priority
        # Break ties with timestamp (older first)
        return self.timestamp < other.timestamp


class IntelligentEventBuffer:
    """
    Intelligent event buffer that prioritizes important events during load shedding.
    """
    
    def __init__(self, 
                 max_buffer_size: int = 10000,
                 priority_calculator: Optional[Callable[[Event], float]] = None,
                 aging_factor: float = 0.1):
        """
        Initialize intelligent event buffer.
        
        Args:
            max_buffer_size: Maximum number of events to buffer
            priority_calculator: Function to calculate event priority
            aging_factor: How much to increase priority as events age in buffer
        """
        self.max_buffer_size = max_buffer_size
        self.priority_calculator = priority_calculator or self._default_priority_calculator
        self.aging_factor = aging_factor
        
        # Priority queue (min-heap, but we use negative priorities for max-heap behavior)
        self._buffer = []
        self._buffer_lock = threading.Lock()
        
        # Statistics
        self.events_buffered = 0
        self.events_dropped_from_buffer = 0
        self.high_priority_events_saved = 0
        
        # Load-adaptive behavior
        self.current_load_level = LoadLevel.NORMAL
        self.load_specific_thresholds = {
            LoadLevel.NORMAL: 1.0,      # Accept all events
            LoadLevel.MEDIUM: 0.7,      # Accept 70%+ priority events
            LoadLevel.HIGH: 0.5,        # Accept 50%+ priority events
            LoadLevel.CRITICAL: 0.8     # Only very high priority events
        }
        
    def add_event(self, event: Event, current_load: LoadLevel) -> bool:
        """
        Add event to buffer, potentially dropping low-priority events.
        
        Args:
            event: Event to add
            current_load: Current system load level
            
        Returns:
            bool: True if event was buffered, False if dropped
        """
        self.current_load_level = current_load
        priority = self.priority_calculator(event)
        
        # Check if event meets minimum priority threshold for current load
        min_threshold = self.load_specific_thresholds[current_load]
        if priority < min_threshold:
            return False  # Drop immediately
        
        with self._buffer_lock:
            # If buffer is full, need to make space
            if len(self._buffer) >= self.max_buffer_size:
                if not self._make_space_for_event(priority):
                    self.events_dropped_from_buffer += 1
                    return False
            
            # Add event to buffer
            priority_event = PriorityEvent(event, priority, event.timestamp)
            heapq.heappush(self._buffer, priority_event)
            self.events_buffered += 1
            
            return True
    
    def get_next_event(self) -> Optional[Event]:
        """Get the highest priority event from buffer."""
        with self._buffer_lock:
            if not self._buffer:
                return None
            
            # Age-boost priorities before getting next event
            self._update_priorities_for_aging()
            
            priority_event = heapq.heappop(self._buffer)
            return priority_event.event
    
    def _make_space_for_event(self, new_event_priority: float) -> bool:
        """
        Make space in buffer for a new event by potentially dropping low-priority events.
        
        Args:
            new_event_priority: Priority of the incoming event
            
        Returns:
            bool: True if space was made, False if new event should be dropped
        """
        if not self._buffer:
            return True
        
        # Find lowest priority event in buffer
        lowest_priority_event = min(self._buffer, key=lambda x: x.priority)
        
        # If new event has higher priority, remove lowest priority event
        if new_event_priority > lowest_priority_event.priority:
            self._buffer.remove(lowest_priority_event)
            heapq.heapify(self._buffer)  # Restore heap property
            return True
        
        return False  # Don't add new event
    
    def _update_priorities_for_aging(self):
        """Update event priorities based on how long they've been in buffer."""
        current_time = datetime.now()
        
        for priority_event in self._buffer:
            age_seconds = (current_time - priority_event.buffer_entry_time).total_seconds()
            age_boost = self.aging_factor * (age_seconds / 60)  # Boost per minute
            priority_event.priority += age_boost
        
        # Re-heapify after priority changes
        heapq.heapify(self._buffer)
    
    def _default_priority_calculator(self, event: Event) -> float:
        """
        Default priority calculator based on event characteristics.
        
        Args:
            event: Event to calculate priority for
            
        Returns:
            float: Priority value (0.0 to 1.0)
        """
        priority = 0.5  # Base priority
        
        # Boost priority based on event type
        if hasattr(event, 'type'):
            if event.type in ['critical', 'error', 'alarm']:
                priority += 0.3
            elif event.type in ['warning', 'important']:
                priority += 0.2
            elif event.type in ['info', 'debug']:
                priority -= 0.1
        
        # Time-based priority (recent events slightly more important)
        if hasattr(event, 'timestamp'):
            age_minutes = (datetime.now() - event.timestamp).total_seconds() / 60
            if age_minutes < 1:
                priority += 0.1  # Very recent events
            elif age_minutes > 10:
                priority -= 0.1  # Old events less important
        
        # Event size/complexity (more complex events might be more important)
        if hasattr(event, 'data') and event.data:
            data_size = len(str(event.data))
            if data_size > 1000:
                priority += 0.1  # Large events might be more important
        
        return max(0.0, min(1.0, priority))  # Clamp to [0, 1]
    
    def get_buffer_statistics(self) -> dict:
        """Get buffer statistics for monitoring."""
        with self._buffer_lock:
            return {
                'buffer_size': len(self._buffer),
                'max_buffer_size': self.max_buffer_size,
                'utilization': len(self._buffer) / self.max_buffer_size,
                'events_buffered': self.events_buffered,
                'events_dropped_from_buffer': self.events_dropped_from_buffer,
                'high_priority_saved': self.high_priority_events_saved,
                'current_load_threshold': self.load_specific_thresholds[self.current_load_level]
            }
    
    def clear_buffer(self):
        """Clear all events from buffer."""
        with self._buffer_lock:
            self._buffer.clear()
    
    def set_priority_calculator(self, calculator: Callable[[Event], float]):
        """Set custom priority calculator function."""
        self.priority_calculator = calculator
