"""
Enhanced load monitoring with predictive capabilities and pattern-aware thresholds.
"""

import psutil
import time
import numpy as np
from typing import Dict, List, Optional, Tuple
from collections import deque, defaultdict
from datetime import datetime, timedelta
from enum import Enum

from .LoadMonitor import LoadLevel, LoadMonitor


class PredictiveLoadMonitor(LoadMonitor):
    """
    Enhanced load monitor that predicts future load and adjusts thresholds dynamically.
    """
    
    def __init__(self, memory_threshold=0.8, cpu_threshold=0.9, 
                 queue_threshold=10000, latency_threshold_ms=100,
                 monitoring_interval_sec=1, prediction_window=300):
        super().__init__(memory_threshold, cpu_threshold, queue_threshold, 
                        latency_threshold_ms, monitoring_interval_sec)
        
        # Predictive capabilities
        self.prediction_window = prediction_window  # 5 minutes
        self.load_history = deque(maxlen=1000)
        self.throughput_history = deque(maxlen=100)
        self.pattern_load_history = defaultdict(lambda: deque(maxlen=50))
        
        # Time-based load patterns
        self.hourly_load_patterns = defaultdict(list)
        self.daily_load_patterns = defaultdict(list)
        
        # Adaptive thresholds
        self.base_memory_threshold = memory_threshold
        self.base_cpu_threshold = cpu_threshold
        self.adaptive_factor = 0.1
        
        # Pattern-specific monitoring
        self.pattern_completion_rates = defaultdict(float)
        self.pattern_resource_usage = defaultdict(dict)
    
    def get_current_load_level(self, current_queue_size: int = 0, 
                              current_latency_ms: float = 0.0,
                              pattern_metrics: Dict = None) -> LoadLevel:
        """
        Get current load level with predictive analysis and pattern awareness.
        """
        # Get base metrics
        cpu_usage = psutil.cpu_percent(interval=0.1)
        memory_info = psutil.virtual_memory()
        memory_usage = memory_info.percent / 100.0
        
        # Record current metrics
        current_time = time.time()
        self.load_history.append({
            'timestamp': current_time,
            'cpu': cpu_usage / 100.0,
            'memory': memory_usage,
            'queue_size': current_queue_size,
            'latency': current_latency_ms
        })
        
        # Calculate predicted load
        predicted_load = self._predict_future_load()
        
        # Get adaptive thresholds
        adaptive_memory_threshold, adaptive_cpu_threshold = self._get_adaptive_thresholds()
        
        # Determine load level with prediction
        current_load_score = self._calculate_load_score(
            cpu_usage / 100.0, memory_usage, current_queue_size, 
            current_latency_ms, adaptive_memory_threshold, adaptive_cpu_threshold
        )
        
        predicted_load_score = self._calculate_load_score(
            predicted_load.get('cpu', cpu_usage / 100.0),
            predicted_load.get('memory', memory_usage),
            predicted_load.get('queue_size', current_queue_size),
            predicted_load.get('latency', current_latency_ms),
            adaptive_memory_threshold, adaptive_cpu_threshold
        )
        
        # Use the higher of current and predicted load
        final_load_score = max(current_load_score, predicted_load_score * 0.8)
        
        # Apply pattern-specific adjustments
        if pattern_metrics:
            final_load_score = self._adjust_for_patterns(final_load_score, pattern_metrics)
        
        # Determine load level
        if final_load_score >= 0.8:
            self.current_load_level = LoadLevel.CRITICAL
        elif final_load_score >= 0.6:
            self.current_load_level = LoadLevel.HIGH
        elif final_load_score >= 0.3:
            self.current_load_level = LoadLevel.MEDIUM
        else:
            self.current_load_level = LoadLevel.NORMAL
        
        return self.current_load_level
    
    def _predict_future_load(self) -> Dict:
        """Predict future load based on historical patterns."""
        if len(self.load_history) < 10:
            return {}
        
        # Simple linear prediction based on recent trend
        recent_data = list(self.load_history)[-10:]
        
        # Calculate trends
        timestamps = [d['timestamp'] for d in recent_data]
        cpu_values = [d['cpu'] for d in recent_data]
        memory_values = [d['memory'] for d in recent_data]
        queue_values = [d['queue_size'] for d in recent_data]
        latency_values = [d['latency'] for d in recent_data]
        
        # Simple linear regression for trend
        def predict_trend(values, timestamps, future_time):
            if len(values) < 2:
                return values[-1] if values else 0
            
            # Calculate slope
            n = len(values)
            sum_x = sum(timestamps)
            sum_y = sum(values)
            sum_xy = sum(x * y for x, y in zip(timestamps, values))
            sum_x2 = sum(x * x for x in timestamps)
            
            if n * sum_x2 - sum_x * sum_x == 0:
                return values[-1]
            
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
            intercept = (sum_y - slope * sum_x) / n
            
            return max(0, slope * future_time + intercept)
        
        future_time = timestamps[-1] + 60  # Predict 1 minute ahead
        
        return {
            'cpu': min(1.0, predict_trend(cpu_values, timestamps, future_time)),
            'memory': min(1.0, predict_trend(memory_values, timestamps, future_time)),
            'queue_size': max(0, predict_trend(queue_values, timestamps, future_time)),
            'latency': max(0, predict_trend(latency_values, timestamps, future_time))
        }
    
    def _get_adaptive_thresholds(self) -> Tuple[float, float]:
        """Get adaptive thresholds based on historical performance."""
        if len(self.load_history) < 50:
            return self.base_memory_threshold, self.base_cpu_threshold
        
        # Calculate average resource usage
        recent_data = list(self.load_history)[-50:]
        avg_memory = np.mean([d['memory'] for d in recent_data])
        avg_cpu = np.mean([d['cpu'] for d in recent_data])
        
        # Adjust thresholds based on historical usage
        memory_adjustment = (avg_memory - 0.5) * self.adaptive_factor
        cpu_adjustment = (avg_cpu - 0.5) * self.adaptive_factor
        
        adaptive_memory = max(0.5, min(0.95, self.base_memory_threshold + memory_adjustment))
        adaptive_cpu = max(0.5, min(0.95, self.base_cpu_threshold + cpu_adjustment))
        
        return adaptive_memory, adaptive_cpu
    
    def _calculate_load_score(self, cpu: float, memory: float, queue_size: int, 
                             latency: float, mem_threshold: float, cpu_threshold: float) -> float:
        """Calculate composite load score."""
        # Normalize metrics
        cpu_score = min(1.0, cpu / cpu_threshold)
        memory_score = min(1.0, memory / mem_threshold)
        queue_score = min(1.0, queue_size / self.queue_threshold)
        latency_score = min(1.0, latency / self.latency_threshold_ms)
        
        # Weighted combination
        weights = [0.3, 0.3, 0.2, 0.2]  # CPU, Memory, Queue, Latency
        scores = [cpu_score, memory_score, queue_score, latency_score]
        
        return sum(w * s for w, s in zip(weights, scores))
    
    def _adjust_for_patterns(self, base_load_score: float, pattern_metrics: Dict) -> float:
        """Adjust load score based on pattern-specific metrics."""
        adjustment = 0.0
        
        # Adjust based on pattern completion rates
        for pattern_name, completion_rate in pattern_metrics.get('completion_rates', {}).items():
            if completion_rate < 0.5:  # Low completion rate indicates stress
                adjustment += 0.1
        
        # Adjust based on partial match counts
        partial_matches = pattern_metrics.get('partial_matches', 0)
        if partial_matches > 1000:
            adjustment += 0.15
        elif partial_matches > 500:
            adjustment += 0.1
        
        # Adjust based on pattern diversity
        active_patterns = pattern_metrics.get('active_patterns', 1)
        if active_patterns > 5:
            adjustment += 0.05
        
        return min(1.0, base_load_score + adjustment)
