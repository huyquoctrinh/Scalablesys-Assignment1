"""
Configuration class for load shedding parameters.
This module provides configuration management for the load shedding system.
"""

from typing import Dict, Optional
from .LoadMonitor import LoadLevel


class LoadSheddingConfig:
    """
    Configuration class for load shedding system parameters.
    
    This class encapsulates all configurable parameters for the load shedding
    system, making it easy to adjust behavior without code changes.
    """
    
    def __init__(self,
                 enabled: bool = True,
                 strategy_name: str = "probabilistic",
                 memory_threshold: float = 0.8,
                 cpu_threshold: float = 0.9,
                 queue_threshold: int = 10000,
                 latency_threshold_ms: float = 100,
                 monitoring_interval_sec: float = 1.0,
                 drop_probabilities: Optional[Dict[LoadLevel, float]] = None,
                 pattern_priorities: Optional[Dict[str, float]] = None,
                 importance_attributes: Optional[list] = None,
                 adaptive_learning_rate: float = 0.1):
        """
        Initialize load shedding configuration.
        
        Args:
            enabled: Whether load shedding is enabled
            strategy_name: Name of the load shedding strategy to use
            memory_threshold: Memory usage threshold for high load (0.0-1.0)
            cpu_threshold: CPU usage threshold for high load (0.0-1.0)
            queue_threshold: Maximum queue size before considering high load
            latency_threshold_ms: Processing latency threshold in milliseconds
            monitoring_interval_sec: Interval between load monitoring checks
            drop_probabilities: Custom drop probabilities for probabilistic strategy
            pattern_priorities: Pattern priorities for semantic strategy
            importance_attributes: Event attributes indicating importance
            adaptive_learning_rate: Learning rate for adaptive strategy
        """
        self.enabled = enabled
        self.strategy_name = strategy_name.lower()
        
        # Load monitoring parameters
        self.memory_threshold = self._validate_threshold(memory_threshold, "memory_threshold")
        self.cpu_threshold = self._validate_threshold(cpu_threshold, "cpu_threshold")
        self.queue_threshold = max(1, int(queue_threshold))
        self.latency_threshold_ms = max(0.0, float(latency_threshold_ms))
        self.monitoring_interval_sec = max(0.1, float(monitoring_interval_sec))
        
        # Strategy-specific parameters
        self.drop_probabilities = drop_probabilities or self._get_default_drop_probabilities()
        self.pattern_priorities = pattern_priorities or {}
        self.importance_attributes = importance_attributes or ['importance', 'priority', 'critical']
        self.adaptive_learning_rate = max(0.001, min(1.0, float(adaptive_learning_rate)))
        
        # Validate strategy name
        valid_strategies = ['none', 'probabilistic', 'semantic', 'adaptive']
        if self.strategy_name not in valid_strategies:
            raise ValueError(f"Invalid strategy name: {strategy_name}. Valid options: {valid_strategies}")
    
    @staticmethod
    def _validate_threshold(value: float, name: str) -> float:
        """Validate threshold values are between 0.0 and 1.0."""
        try:
            threshold = float(value)
            if not 0.0 <= threshold <= 1.0:
                raise ValueError(f"{name} must be between 0.0 and 1.0")
            return threshold
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid {name}: {value}. Must be a number between 0.0 and 1.0") from e
    
    @staticmethod
    def _get_default_drop_probabilities() -> Dict[LoadLevel, float]:
        """Get default drop probabilities for probabilistic strategy."""
        return {
            LoadLevel.NORMAL: 0.0,
            LoadLevel.MEDIUM: 0.1,
            LoadLevel.HIGH: 0.3,
            LoadLevel.CRITICAL: 0.6
        }
    
    def to_dict(self) -> dict:
        """Convert configuration to dictionary."""
        return {
            'enabled': self.enabled,
            'strategy_name': self.strategy_name,
            'memory_threshold': self.memory_threshold,
            'cpu_threshold': self.cpu_threshold,
            'queue_threshold': self.queue_threshold,
            'latency_threshold_ms': self.latency_threshold_ms,
            'monitoring_interval_sec': self.monitoring_interval_sec,
            'drop_probabilities': {level.value: prob for level, prob in self.drop_probabilities.items()},
            'pattern_priorities': self.pattern_priorities,
            'importance_attributes': self.importance_attributes,
            'adaptive_learning_rate': self.adaptive_learning_rate
        }
    
    @classmethod
    def from_dict(cls, config_dict: dict) -> 'LoadSheddingConfig':
        """Create configuration from dictionary."""
        # Convert drop probabilities back to LoadLevel enum keys
        drop_probs = config_dict.get('drop_probabilities', {})
        if drop_probs:
            drop_probabilities = {}
            for level_str, prob in drop_probs.items():
                try:
                    level = LoadLevel(level_str)
                    drop_probabilities[level] = prob
                except ValueError:
                    continue  # Skip invalid load levels
        else:
            drop_probabilities = None
        
        return cls(
            enabled=config_dict.get('enabled', True),
            strategy_name=config_dict.get('strategy_name', 'probabilistic'),
            memory_threshold=config_dict.get('memory_threshold', 0.8),
            cpu_threshold=config_dict.get('cpu_threshold', 0.9),
            queue_threshold=config_dict.get('queue_threshold', 10000),
            latency_threshold_ms=config_dict.get('latency_threshold_ms', 100),
            monitoring_interval_sec=config_dict.get('monitoring_interval_sec', 1.0),
            drop_probabilities=drop_probabilities,
            pattern_priorities=config_dict.get('pattern_priorities', {}),
            importance_attributes=config_dict.get('importance_attributes', None),
            adaptive_learning_rate=config_dict.get('adaptive_learning_rate', 0.1)
        )
    
    def update(self, **kwargs):
        """Update configuration parameters."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key in ['memory_threshold', 'cpu_threshold']:
                    setattr(self, key, self._validate_threshold(value, key))
                elif key == 'queue_threshold':
                    setattr(self, key, max(1, int(value)))
                elif key == 'strategy_name':
                    valid_strategies = ['none', 'probabilistic', 'semantic', 'adaptive']
                    if value.lower() not in valid_strategies:
                        raise ValueError(f"Invalid strategy name: {value}")
                    setattr(self, key, value.lower())
                else:
                    setattr(self, key, value)
            else:
                raise ValueError(f"Unknown configuration parameter: {key}")
    
    def __str__(self) -> str:
        """String representation of configuration."""
        return (f"LoadSheddingConfig(enabled={self.enabled}, "
                f"strategy={self.strategy_name}, "
                f"mem_threshold={self.memory_threshold}, "
                f"cpu_threshold={self.cpu_threshold})")
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return str(self.to_dict())


# Predefined configurations for common scenarios
class PresetConfigs:
    """Predefined load shedding configurations for common use cases."""
    
    @staticmethod
    def conservative() -> LoadSheddingConfig:
        """Conservative load shedding - only drops under severe load."""
        return LoadSheddingConfig(
            memory_threshold=0.9,
            cpu_threshold=0.95,
            queue_threshold=20000,
            latency_threshold_ms=200,
            drop_probabilities={
                LoadLevel.NORMAL: 0.0,
                LoadLevel.MEDIUM: 0.05,
                LoadLevel.HIGH: 0.15,
                LoadLevel.CRITICAL: 0.4
            }
        )
    
    @staticmethod
    def aggressive() -> LoadSheddingConfig:
        """Aggressive load shedding - starts dropping earlier."""
        return LoadSheddingConfig(
            memory_threshold=0.6,
            cpu_threshold=0.7,
            queue_threshold=5000,
            latency_threshold_ms=50,
            drop_probabilities={
                LoadLevel.NORMAL: 0.0,
                LoadLevel.MEDIUM: 0.2,
                LoadLevel.HIGH: 0.5,
                LoadLevel.CRITICAL: 0.8
            }
        )
    
    @staticmethod
    def quality_preserving() -> LoadSheddingConfig:
        """Quality-preserving configuration using semantic strategy."""
        return LoadSheddingConfig(
            strategy_name="semantic",
            memory_threshold=0.8,
            cpu_threshold=0.85,
            importance_attributes=['priority', 'importance', 'critical', 'severity']
        )
    
    @staticmethod
    def adaptive_learning() -> LoadSheddingConfig:
        """Adaptive configuration that learns from system behavior."""
        return LoadSheddingConfig(
            strategy_name="adaptive",
            adaptive_learning_rate=0.05,
            monitoring_interval_sec=0.5
        )
    
    @staticmethod
    def disabled() -> LoadSheddingConfig:
        """Disabled load shedding for baseline measurements."""
        return LoadSheddingConfig(enabled=False, strategy_name="none")