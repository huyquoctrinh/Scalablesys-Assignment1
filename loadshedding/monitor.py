# loadshedding/monitor.py
"""
System monitoring for overload detection.
Tracks throughput, latency, and queue sizes.
"""

import numpy as np
from datetime import datetime
from collections import deque
import logging

logger = logging.getLogger(__name__)


class OverloadMonitor:
    """Monitor system overload and collect metrics"""
    
    def __init__(self, window_size=100, latency_threshold=0.1, queue_threshold=1000):
        """
        Args:
            window_size: Number of recent events to track
            latency_threshold: Latency threshold in seconds
            queue_threshold: Queue size threshold
        """
        self.window_size = window_size
        self.latency_threshold = latency_threshold
        self.queue_threshold = queue_threshold
        
        # Use deques for efficient sliding windows
        self.event_times = deque(maxlen=window_size)
        self.processing_times = deque(maxlen=window_size)
        self.queue_sizes = deque(maxlen=window_size)
        self.partial_match_counts = deque(maxlen=window_size)
        
        self.overload_detected = False
        self.overload_count = 0
        
    def record_event(self, arrival_time, processing_time, queue_size, partial_match_count):
        """
        Record metrics for each event.
        
        Args:
            arrival_time: Event arrival timestamp
            processing_time: Time to process event (seconds)
            queue_size: Current queue size
            partial_match_count: Number of partial matches
        """
        self.event_times.append(arrival_time)
        self.processing_times.append(processing_time)
        self.queue_sizes.append(queue_size)
        self.partial_match_counts.append(partial_match_count)
    
    def check_overload(self):
        """
        Detect if system is overloaded.
        
        Returns:
            bool: True if overloaded
        """
        if len(self.processing_times) < 10:
            return False
        
        # Check recent average latency and queue size
        recent_latency = np.mean(list(self.processing_times)[-10:])
        recent_queue = np.mean(list(self.queue_sizes)[-10:])
        
        overloaded = (recent_latency > self.latency_threshold or 
                     recent_queue > self.queue_threshold)
        
        if overloaded and not self.overload_detected:
            self.overload_count += 1
            logger.warning(f"OVERLOAD DETECTED at {datetime.now()}")
            logger.warning(f"  Avg Latency: {recent_latency*1000:.2f}ms")
            logger.warning(f"  Avg Queue Size: {recent_queue:.0f}")
            
        self.overload_detected = overloaded
        return overloaded
    
    def get_throughput(self):
        """
        Calculate current throughput (events/sec).
        
        Returns:
            float: Events per second
        """
        if len(self.event_times) < 2:
            return 0
        
        time_span = (self.event_times[-1] - self.event_times[0]).total_seconds()
        return len(self.event_times) / time_span if time_span > 0 else 0
    
    def get_statistics(self):
        """Get current performance statistics"""
        if not self.processing_times:
            return {
                'throughput_eps': 0,
                'avg_latency': 0,
                'p95_latency': 0,
                'avg_queue_size': 0,
                'avg_partial_matches': 0,
                'overload_detected': False
            }
        
        return {
            'throughput_eps': self.get_throughput(),
            'avg_latency': np.mean(self.processing_times),
            'p95_latency': np.percentile(list(self.processing_times), 95),
            'p99_latency': np.percentile(list(self.processing_times), 99),
            'avg_queue_size': np.mean(self.queue_sizes),
            'avg_partial_matches': np.mean(self.partial_match_counts),
            'overload_detected': self.overload_detected,
            'overload_count': self.overload_count
        }


class BurstyWorkloadGenerator:
    """Generate bursty event streams for testing"""
    
    def __init__(self, base_rate=100, burst_rate=1000, 
                 burst_duration=10, burst_interval=60):
        """
        Args:
            base_rate: events/second during normal periods
            burst_rate: events/second during burst periods
            burst_duration: seconds of burst
            burst_interval: seconds between bursts
        """
        self.base_rate = base_rate
        self.burst_rate = burst_rate
        self.burst_duration = burst_duration
        self.burst_interval = burst_interval
        
    def is_burst_period(self, time_offset):
        """
        Determine if given time offset is in burst period.
        
        Args:
            time_offset: Seconds from start
            
        Returns:
            bool: True if in burst
        """
        cycle_position = time_offset % self.burst_interval
        return cycle_position < self.burst_duration
    
    def get_rate_at_time(self, time_offset):
        """Get event rate at given time offset"""
        return self.burst_rate if self.is_burst_period(time_offset) else self.base_rate