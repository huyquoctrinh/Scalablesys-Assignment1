"""
Real-time adaptive configuration system for load shedding.
This module provides dynamic parameter tuning based on system performance feedback.
"""

import json
import threading
import time
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from .LoadSheddingConfig import LoadSheddingConfig
from .LoadMonitor import LoadLevel


@dataclass
class PerformanceMetrics:
    """Performance metrics for adaptive tuning."""
    throughput_eps: float = 0.0
    average_latency_ms: float = 0.0
    drop_rate: float = 0.0
    cpu_utilization: float = 0.0
    memory_utilization: float = 0.0
    quality_score: float = 1.0  # Pattern matching quality
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class AdaptationRule:
    """Rule for adapting configuration based on performance."""
    name: str
    condition: Callable[[PerformanceMetrics], bool]
    adaptation: Callable[[LoadSheddingConfig], LoadSheddingConfig]
    priority: int = 1
    cooldown_minutes: int = 5
    last_applied: Optional[datetime] = None


class AdaptiveConfigurationManager:
    """
    Manages real-time adaptation of load shedding configuration based on performance feedback.
    """
    
    def __init__(self, 
                 initial_config: LoadSheddingConfig,
                 adaptation_interval_seconds: int = 30,
                 performance_history_size: int = 100):
        """
        Initialize adaptive configuration manager.
        
        Args:
            initial_config: Initial load shedding configuration
            adaptation_interval_seconds: How often to check for adaptations
            performance_history_size: Number of performance samples to keep
        """
        self.current_config = initial_config
        self.adaptation_interval = timedelta(seconds=adaptation_interval_seconds)
        self.performance_history_size = performance_history_size
        
        # Performance tracking
        self.performance_history: List[PerformanceMetrics] = []
        self.performance_lock = threading.Lock()
        
        # Adaptation rules
        self.adaptation_rules: List[AdaptationRule] = []
        self._setup_default_adaptation_rules()
        
        # Configuration change callbacks
        self.config_change_callbacks: List[Callable[[LoadSheddingConfig], None]] = []
        
        # Adaptation control
        self.adaptation_enabled = True
        self.adaptation_thread = None
        self.stop_adaptation = threading.Event()
        
        # Statistics
        self.total_adaptations = 0
        self.adaptation_history = []
        
    def start_adaptive_tuning(self):
        """Start the adaptive tuning process in a background thread."""
        if self.adaptation_thread is None or not self.adaptation_thread.is_alive():
            self.stop_adaptation.clear()
            self.adaptation_thread = threading.Thread(target=self._adaptation_loop, daemon=True)
            self.adaptation_thread.start()
    
    def stop_adaptive_tuning(self):
        """Stop the adaptive tuning process."""
        if self.adaptation_thread and self.adaptation_thread.is_alive():
            self.stop_adaptation.set()
            self.adaptation_thread.join(timeout=5)
    
    def update_performance_metrics(self, metrics: PerformanceMetrics):
        """Update performance metrics for adaptation decisions."""
        with self.performance_lock:
            self.performance_history.append(metrics)
            
            # Keep only recent history
            if len(self.performance_history) > self.performance_history_size:
                self.performance_history = self.performance_history[-self.performance_history_size:]
    
    def _adaptation_loop(self):
        """Main adaptation loop running in background thread."""
        while not self.stop_adaptation.is_set():
            try:
                if self.adaptation_enabled:
                    self._check_and_apply_adaptations()
                
                # Wait for next check
                self.stop_adaptation.wait(self.adaptation_interval.total_seconds())
                
            except Exception as e:
                print(f"Error in adaptation loop: {e}")
                time.sleep(5)  # Wait a bit before retrying
    
    def _check_and_apply_adaptations(self):
        """Check all adaptation rules and apply applicable ones."""
        if len(self.performance_history) < 3:
            return  # Need some history for decisions
        
        with self.performance_lock:
            recent_metrics = self.performance_history[-5:]  # Last 5 measurements
            current_metrics = self.performance_history[-1]
        
        # Sort rules by priority (higher priority first)
        sorted_rules = sorted(self.adaptation_rules, key=lambda r: r.priority, reverse=True)
        
        for rule in sorted_rules:
            # Check cooldown
            if rule.last_applied:
                cooldown_period = timedelta(minutes=rule.cooldown_minutes)
                if datetime.now() - rule.last_applied < cooldown_period:
                    continue
            
            # Check condition
            try:
                if rule.condition(current_metrics):
                    # Apply adaptation
                    old_config = self.current_config
                    new_config = rule.adaptation(self.current_config)
                    
                    if self._config_changed(old_config, new_config):
                        self.current_config = new_config
                        rule.last_applied = datetime.now()
                        
                        # Record adaptation
                        self._record_adaptation(rule.name, old_config, new_config)
                        
                        # Notify callbacks
                        self._notify_config_change(new_config)
                        
                        break  # Apply only one rule per iteration
                        
            except Exception as e:
                print(f"Error applying adaptation rule {rule.name}: {e}")
    
    def _setup_default_adaptation_rules(self):
        """Setup default adaptation rules."""
        
        # Rule 1: Increase monitoring frequency under high load
        self.adaptation_rules.append(AdaptationRule(
            name="increase_monitoring_frequency",
            condition=lambda m: m.cpu_utilization > 0.8 or m.memory_utilization > 0.8,
            adaptation=lambda c: self._adjust_monitoring_frequency(c, factor=0.5),
            priority=3,
            cooldown_minutes=2
        ))
        
        # Rule 2: Adjust drop thresholds based on quality impact
        self.adaptation_rules.append(AdaptationRule(
            name="adjust_drop_thresholds",
            condition=lambda m: m.quality_score < 0.7 and m.drop_rate > 0.3,
            adaptation=lambda c: self._reduce_aggressiveness(c),
            priority=2,
            cooldown_minutes=5
        ))
        
        # Rule 3: Increase aggressiveness if system is struggling
        self.adaptation_rules.append(AdaptationRule(
            name="increase_aggressiveness",
            condition=lambda m: m.average_latency_ms > 500 and m.cpu_utilization > 0.9,
            adaptation=lambda c: self._increase_aggressiveness(c),
            priority=4,
            cooldown_minutes=3
        ))
        
        # Rule 4: Reduce monitoring overhead when system is stable
        self.adaptation_rules.append(AdaptationRule(
            name="reduce_monitoring_overhead",
            condition=lambda m: m.cpu_utilization < 0.3 and m.average_latency_ms < 50,
            adaptation=lambda c: self._adjust_monitoring_frequency(c, factor=2.0),
            priority=1,
            cooldown_minutes=10
        ))
        
        # Rule 5: Adjust strategy based on workload characteristics
        self.adaptation_rules.append(AdaptationRule(
            name="adapt_strategy_to_workload",
            condition=lambda m: self._is_bursty_workload(),
            adaptation=lambda c: self._optimize_for_bursty_workload(c),
            priority=2,
            cooldown_minutes=15
        ))
    
    def _adjust_monitoring_frequency(self, config: LoadSheddingConfig, factor: float) -> LoadSheddingConfig:
        """Adjust monitoring frequency by the given factor."""
        new_config = LoadSheddingConfig(**asdict(config))
        
        # Adjust monitor update interval
        current_interval = getattr(config, 'monitor_update_interval_seconds', 1.0)
        new_interval = max(0.1, min(10.0, current_interval * factor))
        new_config.monitor_update_interval_seconds = new_interval
        
        return new_config
    
    def _reduce_aggressiveness(self, config: LoadSheddingConfig) -> LoadSheddingConfig:
        """Reduce load shedding aggressiveness to preserve quality."""
        new_config = LoadSheddingConfig(**asdict(config))
        
        # Increase thresholds to be less aggressive
        if hasattr(config, 'cpu_threshold'):
            new_config.cpu_threshold = min(0.95, config.cpu_threshold + 0.1)
        if hasattr(config, 'memory_threshold'):
            new_config.memory_threshold = min(0.95, config.memory_threshold + 0.1)
        
        return new_config
    
    def _increase_aggressiveness(self, config: LoadSheddingConfig) -> LoadSheddingConfig:
        """Increase load shedding aggressiveness to protect system."""
        new_config = LoadSheddingConfig(**asdict(config))
        
        # Decrease thresholds to be more aggressive
        if hasattr(config, 'cpu_threshold'):
            new_config.cpu_threshold = max(0.5, config.cpu_threshold - 0.1)
        if hasattr(config, 'memory_threshold'):
            new_config.memory_threshold = max(0.5, config.memory_threshold - 0.1)
        
        return new_config
    
    def _is_bursty_workload(self) -> bool:
        """Detect if current workload is bursty."""
        if len(self.performance_history) < 10:
            return False
        
        recent_throughputs = [m.throughput_eps for m in self.performance_history[-10:]]
        
        # Calculate coefficient of variation
        if len(recent_throughputs) > 1:
            import statistics
            mean_throughput = statistics.mean(recent_throughputs)
            if mean_throughput > 0:
                std_throughput = statistics.stdev(recent_throughputs)
                coefficient_of_variation = std_throughput / mean_throughput
                return coefficient_of_variation > 0.5  # High variability indicates bursty pattern
        
        return False
    
    def _optimize_for_bursty_workload(self, config: LoadSheddingConfig) -> LoadSheddingConfig:
        """Optimize configuration for bursty workloads."""
        new_config = LoadSheddingConfig(**asdict(config))
        
        # Use more reactive strategy for bursty workloads
        new_config.strategy_name = "adaptive"  # Switch to adaptive strategy
        
        # Adjust buffer sizes for bursty patterns
        if hasattr(config, 'buffer_size'):
            new_config.buffer_size = int(config.buffer_size * 1.5)  # Larger buffer for bursts
        
        return new_config
    
    def _config_changed(self, old_config: LoadSheddingConfig, new_config: LoadSheddingConfig) -> bool:
        """Check if configuration actually changed."""
        return asdict(old_config) != asdict(new_config)
    
    def _record_adaptation(self, rule_name: str, old_config: LoadSheddingConfig, new_config: LoadSheddingConfig):
        """Record an adaptation for statistics and logging."""
        adaptation_record = {
            'timestamp': datetime.now(),
            'rule_name': rule_name,
            'old_config_hash': hash(str(asdict(old_config))),
            'new_config_hash': hash(str(asdict(new_config))),
            'performance_snapshot': self.performance_history[-1] if self.performance_history else None
        }
        
        self.adaptation_history.append(adaptation_record)
        self.total_adaptations += 1
        
        print(f"Configuration adapted by rule '{rule_name}' at {adaptation_record['timestamp']}")
    
    def _notify_config_change(self, new_config: LoadSheddingConfig):
        """Notify all registered callbacks about configuration change."""
        for callback in self.config_change_callbacks:
            try:
                callback(new_config)
            except Exception as e:
                print(f"Error in config change callback: {e}")
    
    def add_config_change_callback(self, callback: Callable[[LoadSheddingConfig], None]):
        """Add callback to be notified of configuration changes."""
        self.config_change_callbacks.append(callback)
    
    def add_custom_adaptation_rule(self, rule: AdaptationRule):
        """Add a custom adaptation rule."""
        self.adaptation_rules.append(rule)
    
    def get_current_config(self) -> LoadSheddingConfig:
        """Get current configuration."""
        return self.current_config
    
    def get_adaptation_statistics(self) -> dict:
        """Get statistics about adaptations performed."""
        return {
            'total_adaptations': self.total_adaptations,
            'adaptation_enabled': self.adaptation_enabled,
            'recent_adaptations': self.adaptation_history[-10:] if self.adaptation_history else [],
            'active_rules': len(self.adaptation_rules),
            'performance_history_size': len(self.performance_history),
            'average_performance': self._calculate_average_performance() if self.performance_history else None
        }
    
    def _calculate_average_performance(self) -> dict:
        """Calculate average performance metrics."""
        if not self.performance_history:
            return {}
        
        recent_metrics = self.performance_history[-20:]  # Last 20 measurements
        
        avg_metrics = {
            'throughput_eps': sum(m.throughput_eps for m in recent_metrics) / len(recent_metrics),
            'average_latency_ms': sum(m.average_latency_ms for m in recent_metrics) / len(recent_metrics),
            'drop_rate': sum(m.drop_rate for m in recent_metrics) / len(recent_metrics),
            'cpu_utilization': sum(m.cpu_utilization for m in recent_metrics) / len(recent_metrics),
            'memory_utilization': sum(m.memory_utilization for m in recent_metrics) / len(recent_metrics),
            'quality_score': sum(m.quality_score for m in recent_metrics) / len(recent_metrics)
        }
        
        return avg_metrics
    
    def enable_adaptation(self):
        """Enable adaptive configuration."""
        self.adaptation_enabled = True
    
    def disable_adaptation(self):
        """Disable adaptive configuration."""
        self.adaptation_enabled = False
    
    def force_configuration(self, config: LoadSheddingConfig):
        """Force a specific configuration (bypasses adaptation)."""
        old_config = self.current_config
        self.current_config = config
        self._notify_config_change(config)
        
        # Record manual change
        self._record_adaptation("manual_override", old_config, config)
