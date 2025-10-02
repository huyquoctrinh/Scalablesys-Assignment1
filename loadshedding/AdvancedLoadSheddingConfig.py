"""
Multi-level load shedding configuration with intelligent pattern prioritization.
"""

from typing import Dict, List, Optional
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from .LoadMonitor import LoadLevel
from .LoadSheddingConfig import LoadSheddingConfig


class LoadSheddingLevel(Enum):
    """Different levels of load shedding aggressiveness."""
    NONE = "none"
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"
    EMERGENCY = "emergency"


@dataclass
class PatternPriority:
    """Pattern priority configuration."""
    name: str
    importance: float  # 0.0 - 1.0
    resource_weight: float  # Relative resource usage
    completion_target: float  # Target completion rate
    time_criticality: str  # "rush_hour", "business_hours", "anytime"


class AdvancedLoadSheddingConfig:
    """
    Advanced configuration for multi-level load shedding with pattern prioritization.
    """
    
    def __init__(self):
        self.pattern_priorities = self._get_default_pattern_priorities()
        self.level_configs = self._get_default_level_configs()
        self.time_based_adjustments = self._get_time_based_adjustments()
        self.emergency_patterns = ["HotPathDetection", "CriticalCommute"]
        
    def _get_default_pattern_priorities(self) -> Dict[str, PatternPriority]:
        """Define default pattern priorities for CitiBike analysis."""
        return {
            "HotPathDetection": PatternPriority(
                name="HotPathDetection",
                importance=0.9,
                resource_weight=1.2,
                completion_target=0.8,
                time_criticality="rush_hour"
            ),
            "MorningCommutePattern": PatternPriority(
                name="MorningCommutePattern",
                importance=0.85,
                resource_weight=1.1,
                completion_target=0.75,
                time_criticality="rush_hour"
            ),
            "EveningCommutePattern": PatternPriority(
                name="EveningCommutePattern",
                importance=0.85,
                resource_weight=1.1,
                completion_target=0.75,
                time_criticality="rush_hour"
            ),
            "WeekendLeisurePattern": PatternPriority(
                name="WeekendLeisurePattern",
                importance=0.6,
                resource_weight=0.8,
                completion_target=0.6,
                time_criticality="anytime"
            )
        }
    
    def create_configuration_for_time(self, 
                                current_time: Optional[datetime] = None,
                                load_level: LoadLevel = LoadLevel.NORMAL) -> LoadSheddingConfig:
        """
        Get appropriate load shedding configuration based on current conditions.
        """
        # Map load level to shedding level
        shedding_level = self._map_load_to_shedding_level(load_level)
        
        # Calculate weighted drop probabilities
        total_importance = sum(p.importance for p in self.pattern_priorities.values())
        
        pattern_priorities = {}
        for pattern_name, priority in self.pattern_priorities.items():
            # Normalize importance to 0-10 scale
            pattern_priorities[pattern_name] = (priority.importance / total_importance) * 10.0
        
        # Create semantic load shedding configuration
        return LoadSheddingConfig(
            enabled=True,
            strategy_name="semantic",
            memory_threshold=0.7,
            cpu_threshold=0.8,
            pattern_priorities=pattern_priorities,
            importance_attributes=['importance', 'priority', 'time_criticality']
        )
    
    def _map_load_to_shedding_level(self, load_level: LoadLevel) -> LoadSheddingLevel:
        """Map system load level to load shedding level."""
        mapping = {
            LoadLevel.NORMAL: LoadSheddingLevel.NONE,
            LoadLevel.MEDIUM: LoadSheddingLevel.CONSERVATIVE,
            LoadLevel.HIGH: LoadSheddingLevel.MODERATE,
            LoadLevel.CRITICAL: LoadSheddingLevel.AGGRESSIVE
        }
        return mapping.get(load_level, LoadSheddingLevel.CONSERVATIVE)
