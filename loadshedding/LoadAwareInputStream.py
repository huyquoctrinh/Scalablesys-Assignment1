"""
Load-aware input stream for the OpenCEP system.
This module provides an enhanced input stream that incorporates load shedding capabilities.
"""

from typing import Optional
from stream.Stream import InputStream
from base.Event import Event
from .LoadMonitor import LoadMonitor, LoadLevel
from .LoadSheddingStrategy import LoadSheddingStrategy
from .LoadSheddingMetrics import LoadSheddingMetrics
import time
from datetime import datetime


class LoadAwareInputStream(InputStream):
    """
    Enhanced input stream with load shedding capabilities.
    
    This stream wrapper monitors system load and applies load shedding
    strategies to drop events when the system is under stress.
    """
    
    def __init__(self, 
                 original_stream: InputStream,
                 load_monitor: LoadMonitor,
                 shedding_strategy: LoadSheddingStrategy,
                 metrics_collector: Optional[LoadSheddingMetrics] = None):
        """
        Initialize load-aware input stream.
        
        Args:
            original_stream: The original input stream to wrap
            load_monitor: Load monitoring component
            shedding_strategy: Strategy for deciding which events to drop
            metrics_collector: Optional metrics collector
        """
        self.original_stream = original_stream
        self.load_monitor = load_monitor
        self.shedding_strategy = shedding_strategy
        self.metrics_collector = metrics_collector
        
        # Internal tracking
        self._queue_size_estimate = 0
        self._events_processed_count = 0
        self._last_latency_update = datetime.now()
        self._recent_processing_times = []
        
        # Load shedding state
        self._current_load_level = LoadLevel.NORMAL
        self._last_load_check = datetime.now()
        self._consecutive_drops = 0
        self._max_consecutive_drops = 1000  # Prevent infinite dropping
    
    def get_item(self):
        """
        Get the next event from the stream, applying load shedding if necessary.
        
        Returns:
            Event: The next event, or None if stream is empty
            
        Raises:
            StopIteration: When the stream is exhausted
        """
        start_time = time.time()
        
        try:
            # Get event from original stream
            event = self.original_stream.get_item()
            if event is None:
                raise StopIteration
            
            # Update queue size estimate
            self._queue_size_estimate = max(0, self._queue_size_estimate - 1)
            
            # Check if we should update load assessment
            current_time = datetime.now()
            if (current_time - self._last_load_check).total_seconds() >= 0.1:  # Check every 100ms
                self._update_load_assessment()
                self._last_load_check = current_time
            
            # Decide whether to drop this event
            should_drop = self.shedding_strategy.should_drop_event(event, self._current_load_level)
            
            if should_drop:
                self._handle_dropped_event(event)
                self._consecutive_drops += 1
                
                # Safety mechanism: if we're dropping too many consecutive events,
                # allow some through to prevent complete starvation
                if self._consecutive_drops > self._max_consecutive_drops:
                    self._consecutive_drops = 0
                    should_drop = False
                
                if should_drop:
                    # Recursively get the next event
                    return self.get_item()
            
            # Event is not dropped
            self._consecutive_drops = 0
            self._handle_processed_event(event, start_time)
            
            return event
            
        except StopIteration:
            # Stream is exhausted
            raise
        except Exception as e:
            # Log error but don't break the stream
            print(f"Error in LoadAwareInputStream: {e}")
            # Try to get from original stream directly as fallback
            return self.original_stream.get_item()
    
    def _update_load_assessment(self):
        """Update the current load level assessment."""
        # Estimate processing latency
        avg_latency_ms = 0.0
        if self._recent_processing_times:
            avg_latency_ms = (sum(self._recent_processing_times) / len(self._recent_processing_times)) * 1000
        
        # Update load monitor with current metrics
        self.load_monitor.update_event_metrics(
            queue_size=self._queue_size_estimate,
            events_processed=self._events_processed_count,
            latest_latency_ms=avg_latency_ms
        )
        
        # Get current load level
        self._current_load_level = self.load_monitor.assess_load_level()
        
        # Record system snapshot if metrics collector is available
        if self.metrics_collector:
            monitor_metrics = self.load_monitor.get_current_metrics()
            self.metrics_collector.record_system_snapshot(
                load_level=self._current_load_level.value,
                cpu_percent=monitor_metrics.get('cpu_percent', 0.0),
                memory_percent=monitor_metrics.get('memory_percent', 0.0),
                queue_size=self._queue_size_estimate
            )
    
    def _handle_dropped_event(self, event: Event):
        """Handle an event that was dropped due to load shedding."""
        if self.metrics_collector:
            pattern_name = getattr(event, 'pattern_name', None) or getattr(event, 'event_type', None)
            self.metrics_collector.record_event_dropped(pattern_name)
    
    def _handle_processed_event(self, event: Event, start_time: float):
        """Handle an event that was successfully processed."""
        processing_time = time.time() - start_time
        self._recent_processing_times.append(processing_time)
        
        # Keep only recent processing times (last 20 measurements)
        if len(self._recent_processing_times) > 20:
            self._recent_processing_times = self._recent_processing_times[-20:]
        
        self._events_processed_count += 1
        
        # Record metrics
        if self.metrics_collector:
            pattern_name = getattr(event, 'pattern_name', None) or getattr(event, 'event_type', None)
            self.metrics_collector.record_event_processed(
                pattern_name=pattern_name,
                latency_ms=processing_time * 1000
            )
    
    def has_next(self) -> bool:
        """
        Check if the stream has more events.
        
        Returns:
            bool: True if there are more events available
        """
        return self.original_stream.has_next()
    
    def peek_next(self):
        """
        Peek at the next event without consuming it.
        
        Returns:
            Event: The next event, or None if no events available
        """
        # For load-aware stream, we can't easily peek since we don't know
        # if the next event will be dropped. Return None to indicate
        # uncertainty.
        return None
    
    def get_current_load_level(self) -> LoadLevel:
        """Get the current assessed load level."""
        return self._current_load_level
    
    def get_drop_statistics(self) -> dict:
        """
        Get current drop statistics.
        
        Returns:
            dict: Statistics about dropped events
        """
        return {
            'strategy_name': self.shedding_strategy.name,
            'strategy_drop_rate': self.shedding_strategy.get_drop_rate(),
            'current_load_level': self._current_load_level.value,
            'events_processed': self._events_processed_count,
            'queue_size_estimate': self._queue_size_estimate,
            'consecutive_drops': self._consecutive_drops
        }
    
    def reset_statistics(self):
        """Reset internal statistics and counters."""
        self._events_processed_count = 0
        self._queue_size_estimate = 0
        self._consecutive_drops = 0
        self._recent_processing_times.clear()
        self.shedding_strategy.reset_statistics()
    
    def update_queue_size(self, new_size: int):
        """
        Update the estimated queue size.
        
        This can be called by external components that have better
        knowledge of the actual queue size.
        
        Args:
            new_size: New estimated queue size
        """
        self._queue_size_estimate = max(0, int(new_size))
    
    def force_load_assessment(self) -> LoadLevel:
        """
        Force an immediate load level assessment.
        
        Returns:
            LoadLevel: Current assessed load level
        """
        self._update_load_assessment()
        return self._current_load_level
    
    def __iter__(self):
        """Make the stream iterable."""
        return self
    
    def __next__(self):
        """Support for Python iteration protocol."""
        try:
            return self.get_item()
        except StopIteration:
            raise
    
    def close(self):
        """Close the stream and clean up resources."""
        if hasattr(self.original_stream, 'close'):
            self.original_stream.close()
        
        # Final metrics update
        if self.metrics_collector:
            final_stats = self.get_drop_statistics()
            print(f"LoadAwareInputStream closing - Final stats: {final_stats}")


class BufferedLoadAwareInputStream(LoadAwareInputStream):
    """
    Enhanced version with internal buffering for better load assessment.
    
    This version maintains a small internal buffer to provide better
    load assessment and more accurate queue size estimates.
    """
    
    def __init__(self, 
                 original_stream: InputStream,
                 load_monitor: LoadMonitor,
                 shedding_strategy: LoadSheddingStrategy,
                 metrics_collector: Optional[LoadSheddingMetrics] = None,
                 buffer_size: int = 100):
        """
        Initialize buffered load-aware input stream.
        
        Args:
            buffer_size: Size of internal buffer for load assessment
        """
        super().__init__(original_stream, load_monitor, shedding_strategy, metrics_collector)
        self.buffer_size = buffer_size
        self._internal_buffer = []
        self._buffer_filled = False
    
    def _fill_buffer(self):
        """Fill the internal buffer from the original stream."""
        while len(self._internal_buffer) < self.buffer_size:
            try:
                event = self.original_stream.get_item()
                if event is not None:
                    self._internal_buffer.append(event)
                else:
                    break
            except StopIteration:
                break
        
        self._buffer_filled = True
        self._queue_size_estimate = len(self._internal_buffer)
    
    def get_item(self):
        """Get next event from buffered stream."""
        # Fill buffer on first access
        if not self._buffer_filled:
            self._fill_buffer()
        
        # If buffer is empty, stream is exhausted
        if not self._internal_buffer:
            raise StopIteration
        
        # Continue with normal load-aware processing
        start_time = time.time()
        
        # Get event from buffer
        event = self._internal_buffer.pop(0)
        self._queue_size_estimate = len(self._internal_buffer)
        
        # Try to refill buffer
        try:
            next_event = self.original_stream.get_item()
            if next_event is not None:
                self._internal_buffer.append(next_event)
                self._queue_size_estimate += 1
        except StopIteration:
            pass  # No more events to buffer
        
        # Apply load shedding logic
        current_time = datetime.now()
        if (current_time - self._last_load_check).total_seconds() >= 0.1:
            self._update_load_assessment()
            self._last_load_check = current_time
        
        should_drop = self.shedding_strategy.should_drop_event(event, self._current_load_level)
        
        if should_drop:
            self._handle_dropped_event(event)
            self._consecutive_drops += 1
            
            if self._consecutive_drops <= self._max_consecutive_drops:
                return self.get_item()  # Try next event
            else:
                self._consecutive_drops = 0  # Reset to prevent starvation
        
        # Process the event
        self._consecutive_drops = 0
        self._handle_processed_event(event, start_time)
        
        return event