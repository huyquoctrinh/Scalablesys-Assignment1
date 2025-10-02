"""
Load monitoring component for the OpenCEP load shedding system.
This module provides system resource monitoring and load level assessment.
"""

import psutil
import time
from enum import Enum
from typing import Optional
from datetime import datetime, timedelta


class LoadLevel(Enum):
    """
    Enumeration of system load levels
    """
    NORMAL = "normal"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class LoadMonitor:
    """
    Monitors system resources and event processing metrics to determine load level.
    
    This class tracks CPU usage, memory consumption, event queue sizes, and processing
    latency to provide a comprehensive view of system load.
    """
    
    def __init__(self, memory_threshold=0.8, cpu_threshold=0.9, 
                 queue_threshold=10000, latency_threshold_ms=100,
                 monitoring_interval_sec=1):
        """
        Initialize the load monitor with configurable thresholds.
        
        Args:
            memory_threshold: Memory usage threshold for high load (0.0-1.0)
            cpu_threshold: CPU usage threshold for high load (0.0-1.0)
            queue_threshold: Maximum queue size before considering high load
            latency_threshold_ms: Processing latency threshold in milliseconds
            monitoring_interval_sec: Interval between monitoring checks
        """
        self.memory_threshold = memory_threshold
        self.cpu_threshold = cpu_threshold
        self.queue_threshold = queue_threshold
        self.latency_threshold_ms = latency_threshold_ms
        self.monitoring_interval = monitoring_interval_sec
        
        self.current_load_level = LoadLevel.NORMAL
        self.last_check_time = datetime.now()
        
        # Metrics tracking
        self.recent_latencies = []
        self.event_queue_size = 0
        self.events_processed_last_interval = 0
        self.last_cpu_percent = 0.0
        self.last_memory_percent = 0.0
        
        # Initialize system monitoring
        self._initialize_monitoring()
    
    def _initialize_monitoring(self):
        """Initialize system resource monitoring."""
        try:
            # Get initial readings
            psutil.cpu_percent(interval=None)  # First call returns 0.0, subsequent calls return actual value
            time.sleep(0.1)  # Short delay to get meaningful CPU reading
        except Exception as e:
            print(f"Warning: Could not initialize system monitoring: {e}")
    
    def update_event_metrics(self, queue_size: int, events_processed: int, 
                           latest_latency_ms: Optional[float] = None):
        """
        Update event processing metrics.
        
        Args:
            queue_size: Current size of the event queue
            events_processed: Number of events processed since last update
            latest_latency_ms: Latest processing latency in milliseconds
        """
        self.event_queue_size = queue_size
        self.events_processed_last_interval = events_processed
        
        if latest_latency_ms is not None:
            self.recent_latencies.append(latest_latency_ms)
            # Keep only recent latencies (last 10 measurements)
            if len(self.recent_latencies) > 10:
                self.recent_latencies = self.recent_latencies[-10:]
    
    def assess_load_level(self) -> LoadLevel:
        """
        Assess current system load level based on all available metrics.
        
        Returns:
            LoadLevel: Current assessed load level
        """
        current_time = datetime.now()
        if (current_time - self.last_check_time).total_seconds() < self.monitoring_interval:
            return self.current_load_level
        
        self.last_check_time = current_time
        
        # Get system resource metrics
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1) / 100.0
            memory_percent = psutil.virtual_memory().percent / 100.0
            self.last_cpu_percent = cpu_percent
            self.last_memory_percent = memory_percent
        except Exception as e:
            print(f"Warning: Could not get system metrics, using cached values: {e}")
            cpu_percent = self.last_cpu_percent
            memory_percent = self.last_memory_percent
        
        # Calculate average latency
        avg_latency = sum(self.recent_latencies) / len(self.recent_latencies) if self.recent_latencies else 0
        
        # Assess load level based on multiple factors
        load_indicators = []
        
        # CPU load assessment
        if cpu_percent > self.cpu_threshold:
            load_indicators.append(LoadLevel.CRITICAL)
        elif cpu_percent > self.cpu_threshold * 0.8:
            load_indicators.append(LoadLevel.HIGH)
        elif cpu_percent > self.cpu_threshold * 0.6:
            load_indicators.append(LoadLevel.MEDIUM)
        else:
            load_indicators.append(LoadLevel.NORMAL)
        
        # Memory load assessment
        if memory_percent > self.memory_threshold:
            load_indicators.append(LoadLevel.CRITICAL)
        elif memory_percent > self.memory_threshold * 0.9:
            load_indicators.append(LoadLevel.HIGH)
        elif memory_percent > self.memory_threshold * 0.7:
            load_indicators.append(LoadLevel.MEDIUM)
        else:
            load_indicators.append(LoadLevel.NORMAL)
        
        # Queue size assessment
        if self.event_queue_size > self.queue_threshold:
            load_indicators.append(LoadLevel.CRITICAL)
        elif self.event_queue_size > self.queue_threshold * 0.7:
            load_indicators.append(LoadLevel.HIGH)
        elif self.event_queue_size > self.queue_threshold * 0.4:
            load_indicators.append(LoadLevel.MEDIUM)
        else:
            load_indicators.append(LoadLevel.NORMAL)
        
        # Latency assessment
        if avg_latency > self.latency_threshold_ms * 2:
            load_indicators.append(LoadLevel.CRITICAL)
        elif avg_latency > self.latency_threshold_ms:
            load_indicators.append(LoadLevel.HIGH)
        elif avg_latency > self.latency_threshold_ms * 0.7:
            load_indicators.append(LoadLevel.MEDIUM)
        else:
            load_indicators.append(LoadLevel.NORMAL)
        
        # Determine overall load level (use the highest severity indicator)
        severity_order = [LoadLevel.NORMAL, LoadLevel.MEDIUM, LoadLevel.HIGH, LoadLevel.CRITICAL]
        self.current_load_level = max(load_indicators, key=lambda x: severity_order.index(x))
        
        return self.current_load_level
    
    def get_current_metrics(self) -> dict:
        """
        Get current system metrics as a dictionary.
        
        Returns:
            dict: Current system metrics
        """
        return {
            'load_level': self.current_load_level,
            'cpu_percent': self.last_cpu_percent,
            'memory_percent': self.last_memory_percent,
            'queue_size': self.event_queue_size,
            'avg_latency_ms': sum(self.recent_latencies) / len(self.recent_latencies) if self.recent_latencies else 0,
            'events_processed_last_interval': self.events_processed_last_interval,
            'timestamp': datetime.now().isoformat()
        }
    
    def reset_metrics(self):
        """Reset all collected metrics."""
        self.recent_latencies.clear()
        self.event_queue_size = 0
        self.events_processed_last_interval = 0
        self.current_load_level = LoadLevel.NORMAL