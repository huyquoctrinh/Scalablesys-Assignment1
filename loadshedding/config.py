# loadshedding/config.py
"""
Load shedding configuration classes.
Defines parameters for different load shedding strategies.
"""

from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class LoadSheddingConfig:
    """Configuration for load shedding behavior"""
    enabled: bool = True
    strategy_name: str = 'utility_based'
    memory_threshold: float = 0.75
    max_partial_matches: int = 10000
    target_latency_ratio: float = 0.5
    shedding_rate: float = 0.3
    
    # Pattern-specific priorities
    pattern_priorities: Dict[str, float] = None
    
    # Custom parameters
    utility_weights: Dict[str, float] = None
    
    def __post_init__(self):
        if self.pattern_priorities is None:
            self.pattern_priorities = {}
        if self.utility_weights is None:
            self.utility_weights = {
                'chain_length': 3.0,
                'target_proximity': 10.0,
                'time_urgency': 4.0,
                'importance': 2.0,
                'priority': 1.5
            }


class PresetConfigs:
    """Preset configurations for common scenarios"""
    
    @staticmethod
    def conservative():
        """Conservative shedding - prioritizes recall"""
        return LoadSheddingConfig(
            enabled=True,
            memory_threshold=0.85,
            shedding_rate=0.15,
            target_latency_ratio=0.9
        )
    
    @staticmethod
    def balanced():
        """Balanced approach"""
        return LoadSheddingConfig(
            enabled=True,
            memory_threshold=0.70,
            shedding_rate=0.30,
            target_latency_ratio=0.5
        )
    
    @staticmethod
    def aggressive():
        """Aggressive shedding - prioritizes latency"""
        return LoadSheddingConfig(
            enabled=True,
            memory_threshold=0.50,
            shedding_rate=0.50,
            target_latency_ratio=0.3
        )