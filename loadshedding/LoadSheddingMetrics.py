"""
Metrics collection for load shedding system.
This module provides comprehensive metrics tracking for load shedding performance evaluation.
"""

import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class LoadSheddingSnapshot:
    """Represents a snapshot of system state at a point in time."""
    timestamp: datetime
    load_level: str
    cpu_percent: float
    memory_percent: float
    queue_size: int
    events_processed: int
    events_dropped: int
    avg_latency_ms: float
    throughput_eps: float  # events per second


class LoadSheddingMetrics:
    """
    Comprehensive metrics collection for load shedding evaluation.
    
    This class tracks various performance metrics during load shedding operation,
    providing data for analysis and optimization.
    """
    
    def __init__(self):
        """Initialize metrics collection."""
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.is_collecting = False
        
        # Event processing metrics
        self.events_processed = 0
        self.events_dropped = 0
        self.patterns_matched = 0
        self.total_events_seen = 0
        
        # Timing metrics
        self.processing_latencies: List[float] = []
        self.end_to_end_latencies: List[float] = []
        
        # System resource metrics
        self.snapshots: List[LoadSheddingSnapshot] = []
        
        # Quality metrics
        self.false_negatives = 0  # Missed matches due to shedding
        self.false_positives = 0  # Incorrect matches
        self.pattern_completeness_ratios: Dict[str, float] = {}
        
        # Load shedding effectiveness
        self.load_shedding_activations = 0
        self.recovery_times: List[float] = []
        self.strategy_switches = 0
        
        # Per-pattern metrics
        self.pattern_metrics: Dict[str, Dict] = defaultdict(lambda: {
            'events_seen': 0,
            'events_dropped': 0,
            'matches_found': 0,
            'avg_latency': 0.0
        })
        
        # Time series data for analysis
        self.time_series_data: List[Dict] = []
        
        # Performance tracking
        self._last_snapshot_time = datetime.now()
        self._last_event_count = 0
    
    def start_collection(self):
        """Start metrics collection."""
        self.start_time = datetime.now()
        self.is_collecting = True
        self._last_snapshot_time = self.start_time
        self.reset_metrics()
    
    def stop_collection(self):
        """Stop metrics collection."""
        self.end_time = datetime.now()
        self.is_collecting = False
    
    def reset_metrics(self):
        """Reset all collected metrics."""
        self.events_processed = 0
        self.events_dropped = 0
        self.patterns_matched = 0
        self.total_events_seen = 0
        
        self.processing_latencies.clear()
        self.end_to_end_latencies.clear()
        self.snapshots.clear()
        
        self.false_negatives = 0
        self.false_positives = 0
        self.pattern_completeness_ratios.clear()
        
        self.load_shedding_activations = 0
        self.recovery_times.clear()
        self.strategy_switches = 0
        
        self.pattern_metrics.clear()
        self.time_series_data.clear()
        
        self._last_event_count = 0
    
    def record_event_processed(self, pattern_name: Optional[str] = None, 
                             latency_ms: Optional[float] = None):
        """Record that an event was processed."""
        if not self.is_collecting:
            return
        
        self.events_processed += 1
        self.total_events_seen += 1
        
        if latency_ms is not None:
            self.processing_latencies.append(latency_ms)
        
        if pattern_name:
            self.pattern_metrics[pattern_name]['events_seen'] += 1
            if latency_ms:
                # Update average latency
                metrics = self.pattern_metrics[pattern_name]
                current_avg = metrics['avg_latency']
                count = metrics['events_seen']
                metrics['avg_latency'] = ((current_avg * (count - 1)) + latency_ms) / count
    
    def record_event_dropped(self, pattern_name: Optional[str] = None):
        """Record that an event was dropped."""
        if not self.is_collecting:
            return
        
        self.events_dropped += 1
        self.total_events_seen += 1
        
        if pattern_name:
            self.pattern_metrics[pattern_name]['events_dropped'] += 1
    
    def record_pattern_matched(self, pattern_name: str, end_to_end_latency_ms: Optional[float] = None):
        """Record that a pattern was matched."""
        if not self.is_collecting:
            return
        
        self.patterns_matched += 1
        self.pattern_metrics[pattern_name]['matches_found'] += 1
        
        if end_to_end_latency_ms is not None:
            self.end_to_end_latencies.append(end_to_end_latency_ms)
    
    def record_system_snapshot(self, load_level: str, cpu_percent: float, 
                             memory_percent: float, queue_size: int):
        """Record a system state snapshot."""
        if not self.is_collecting:
            return
        
        current_time = datetime.now()
        time_diff = (current_time - self._last_snapshot_time).total_seconds()
        
        # Calculate throughput since last snapshot
        events_since_last = self.events_processed - self._last_event_count
        throughput_eps = events_since_last / max(time_diff, 0.001)
        
        # Calculate average latency from recent measurements
        recent_latencies = self.processing_latencies[-100:] if self.processing_latencies else [0]
        avg_latency = sum(recent_latencies) / len(recent_latencies)
        
        snapshot = LoadSheddingSnapshot(
            timestamp=current_time,
            load_level=load_level,
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            queue_size=queue_size,
            events_processed=self.events_processed,
            events_dropped=self.events_dropped,
            avg_latency_ms=avg_latency,
            throughput_eps=throughput_eps
        )
        
        self.snapshots.append(snapshot)
        
        # Add to time series data
        self.time_series_data.append({
            'timestamp': current_time.isoformat(),
            'load_level': load_level,
            'cpu_percent': cpu_percent,
            'memory_percent': memory_percent,
            'queue_size': queue_size,
            'throughput_eps': throughput_eps,
            'avg_latency_ms': avg_latency,
            'drop_rate': self.get_current_drop_rate()
        })
        
        # Update tracking variables
        self._last_snapshot_time = current_time
        self._last_event_count = self.events_processed
    
    def record_load_shedding_activation(self):
        """Record that load shedding was activated."""
        if not self.is_collecting:
            return
        self.load_shedding_activations += 1
    
    def record_recovery_time(self, recovery_time_seconds: float):
        """Record system recovery time after high load."""
        if not self.is_collecting:
            return
        self.recovery_times.append(recovery_time_seconds)
    
    def record_strategy_switch(self):
        """Record that the load shedding strategy was changed."""
        if not self.is_collecting:
            return
        self.strategy_switches += 1
    
    def record_quality_metrics(self, false_negatives: int = 0, false_positives: int = 0,
                             pattern_completeness: Optional[Dict[str, float]] = None):
        """Record quality-related metrics."""
        if not self.is_collecting:
            return
        
        self.false_negatives += false_negatives
        self.false_positives += false_positives
        
        if pattern_completeness:
            self.pattern_completeness_ratios.update(pattern_completeness)
    
    def get_duration_seconds(self) -> float:
        """Get total collection duration in seconds."""
        if self.start_time is None:
            return 0.0
        
        end_time = self.end_time or datetime.now()
        return (end_time - self.start_time).total_seconds()
    
    def get_current_drop_rate(self) -> float:
        """Get current overall drop rate."""
        if self.total_events_seen == 0:
            return 0.0
        return self.events_dropped / self.total_events_seen
    
    def get_average_latency(self) -> float:
        """Get average processing latency in milliseconds."""
        if not self.processing_latencies:
            return 0.0
        return sum(self.processing_latencies) / len(self.processing_latencies)
    
    def get_average_throughput(self) -> float:
        """Get average throughput in events per second."""
        duration = self.get_duration_seconds()
        if duration <= 0:
            return 0.0
        return self.events_processed / duration
    
    def get_pattern_statistics(self) -> Dict[str, Dict]:
        """Get per-pattern statistics."""
        stats = {}
        for pattern_name, metrics in self.pattern_metrics.items():
            total_seen = metrics['events_seen'] + metrics['events_dropped']
            drop_rate = metrics['events_dropped'] / total_seen if total_seen > 0 else 0.0
            
            stats[pattern_name] = {
                'events_seen': metrics['events_seen'],
                'events_dropped': metrics['events_dropped'],
                'matches_found': metrics['matches_found'],
                'drop_rate': drop_rate,
                'avg_latency_ms': metrics['avg_latency'],
                'match_rate': metrics['matches_found'] / max(metrics['events_seen'], 1)
            }
        
        return stats
    
    def get_resource_utilization_stats(self) -> Dict:
        """Get resource utilization statistics."""
        if not self.snapshots:
            return {}
        
        cpu_values = [s.cpu_percent for s in self.snapshots]
        memory_values = [s.memory_percent for s in self.snapshots]
        queue_values = [s.queue_size for s in self.snapshots]
        
        return {
            'cpu': {
                'avg': sum(cpu_values) / len(cpu_values),
                'min': min(cpu_values),
                'max': max(cpu_values)
            },
            'memory': {
                'avg': sum(memory_values) / len(memory_values),
                'min': min(memory_values),
                'max': max(memory_values)
            },
            'queue_size': {
                'avg': sum(queue_values) / len(queue_values),
                'min': min(queue_values),
                'max': max(queue_values)
            }
        }
    
    def get_summary_report(self) -> Dict:
        """Get comprehensive summary report."""
        duration = self.get_duration_seconds()
        
        return {
            'collection_period': {
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': self.end_time.isoformat() if self.end_time else None,
                'duration_seconds': duration
            },
            'event_processing': {
                'total_events_seen': self.total_events_seen,
                'events_processed': self.events_processed,
                'events_dropped': self.events_dropped,
                'drop_rate': self.get_current_drop_rate(),
                'patterns_matched': self.patterns_matched,
                'avg_throughput_eps': self.get_average_throughput()
            },
            'latency_metrics': {
                'avg_processing_latency_ms': self.get_average_latency(),
                'avg_end_to_end_latency_ms': (sum(self.end_to_end_latencies) / len(self.end_to_end_latencies)) if self.end_to_end_latencies else 0,
                'latency_samples': len(self.processing_latencies)
            },
            'quality_metrics': {
                'false_negatives': self.false_negatives,
                'false_positives': self.false_positives,
                'pattern_completeness': dict(self.pattern_completeness_ratios)
            },
            'load_shedding': {
                'activations': self.load_shedding_activations,
                'strategy_switches': self.strategy_switches,
                'avg_recovery_time_sec': (sum(self.recovery_times) / len(self.recovery_times)) if self.recovery_times else 0
            },
            'resource_utilization': self.get_resource_utilization_stats(),
            'pattern_statistics': self.get_pattern_statistics(),
            'snapshots_collected': len(self.snapshots)
        }