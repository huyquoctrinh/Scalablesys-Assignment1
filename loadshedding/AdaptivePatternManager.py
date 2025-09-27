"""
Adaptive pattern management for load shedding.
This module provides dynamic pattern activation/deactivation based on system load.
"""

from typing import List, Set, Dict, Optional
from base.Pattern import Pattern
from .LoadMonitor import LoadLevel
import time
from datetime import datetime


class AdaptivePatternManager:
    """
    Manages pattern activation/deactivation based on system load.
    
    This component dynamically enables and disables patterns based on
    current system load to maintain performance while preserving the
    most important pattern detection capabilities.
    """
    
    def __init__(self, patterns: List[Pattern], pattern_priorities: Optional[Dict[str, float]] = None):
        """
        Initialize adaptive pattern manager.
        
        Args:
            patterns: List of patterns to manage
            pattern_priorities: Optional dictionary mapping pattern names to priority values
        """
        self.all_patterns = patterns
        self.pattern_priorities = pattern_priorities or {}
        self.active_patterns: Set[Pattern] = set(patterns)
        
        # Calculate default priorities if not provided
        if not self.pattern_priorities:
            self._calculate_default_priorities()
        
        # Sort patterns by priority for efficient selection
        self._sorted_patterns = self._sort_patterns_by_priority()
        
        # Load level to pattern ratio mapping
        self.load_level_ratios = {
            LoadLevel.NORMAL: 1.0,    # All patterns active
            LoadLevel.MEDIUM: 0.8,    # 80% of patterns active
            LoadLevel.HIGH: 0.5,      # 50% of patterns active  
            LoadLevel.CRITICAL: 0.2   # Only 20% highest priority patterns active
        }
        
        # Tracking
        self.last_adjustment_time = datetime.now()
        self.adjustment_history = []
        self.pattern_performance_stats = {}
        
        # Initialize pattern stats
        for pattern in patterns:
            pattern_name = self._get_pattern_name(pattern)
            self.pattern_performance_stats[pattern_name] = {
                'matches_found': 0,
                'avg_processing_time': 0.0,
                'last_active': datetime.now(),
                'activation_count': 1
            }
    
    def _get_pattern_name(self, pattern: Pattern) -> str:
        """Get a string identifier for a pattern."""
        if hasattr(pattern, 'name') and pattern.name:
            return pattern.name
        elif hasattr(pattern, 'positive_structure'):
            # Try to create a name from the pattern structure
            return f"Pattern_{id(pattern)}"
        else:
            return f"Pattern_{id(pattern)}"
    
    def _calculate_default_priorities(self):
        """Calculate default priorities for patterns based on complexity."""
        for pattern in self.all_patterns:
            pattern_name = self._get_pattern_name(pattern)
            
            # Simple heuristic: more complex patterns get higher priority
            # (assuming they are more valuable for detection)
            complexity_score = self._estimate_pattern_complexity(pattern)
            
            # Normalize to 0-10 range
            priority = min(10.0, max(1.0, complexity_score))
            self.pattern_priorities[pattern_name] = priority
    
    def _estimate_pattern_complexity(self, pattern: Pattern) -> float:
        """Estimate pattern complexity for priority calculation."""
        try:
            complexity = 1.0
            
            # Check if pattern has structure information
            if hasattr(pattern, 'positive_structure'):
                structure = pattern.positive_structure
                
                # Count operators and events in the structure
                if hasattr(structure, 'get_all_events'):
                    complexity += len(structure.get_all_events()) * 0.5
                
                # Add complexity for time window
                if hasattr(pattern, 'time_window') and pattern.time_window:
                    # Longer time windows are more complex
                    complexity += pattern.time_window / 1000.0  # Assuming milliseconds
            
            # Check for conditions
            if hasattr(pattern, 'condition') and pattern.condition:
                complexity += 2.0  # Conditions add complexity
            
            return complexity
            
        except Exception:
            # If we can't estimate complexity, use default
            return 5.0
    
    def _sort_patterns_by_priority(self) -> List[Pattern]:
        """Sort patterns by priority (highest first)."""
        return sorted(
            self.all_patterns,
            key=lambda p: self.pattern_priorities.get(self._get_pattern_name(p), 0.0),
            reverse=True
        )
    
    def adjust_patterns_for_load(self, load_level: LoadLevel) -> Set[Pattern]:
        """
        Adjust active patterns based on current load level.
        
        Args:
            load_level: Current system load level
            
        Returns:
            Set[Pattern]: Set of patterns that should be active
        """
        current_time = datetime.now()
        
        # Get target ratio for this load level
        target_ratio = self.load_level_ratios.get(load_level, 1.0)
        
        # Calculate how many patterns to keep active
        total_patterns = len(self.all_patterns)
        target_count = max(1, int(total_patterns * target_ratio))
        
        # Select top priority patterns
        new_active_patterns = set(self._sorted_patterns[:target_count])
        
        # Record the adjustment
        if new_active_patterns != self.active_patterns:
            self.adjustment_history.append({
                'timestamp': current_time,
                'load_level': load_level.value,
                'old_count': len(self.active_patterns),
                'new_count': len(new_active_patterns),
                'deactivated': [self._get_pattern_name(p) for p in self.active_patterns - new_active_patterns],
                'activated': [self._get_pattern_name(p) for p in new_active_patterns - self.active_patterns]
            })
            
            # Update pattern stats
            for pattern in new_active_patterns - self.active_patterns:
                pattern_name = self._get_pattern_name(pattern)
                if pattern_name in self.pattern_performance_stats:
                    self.pattern_performance_stats[pattern_name]['activation_count'] += 1
                    self.pattern_performance_stats[pattern_name]['last_active'] = current_time
        
        self.active_patterns = new_active_patterns
        self.last_adjustment_time = current_time
        
        return new_active_patterns
    
    def get_active_patterns(self) -> Set[Pattern]:
        """Get currently active patterns."""
        return self.active_patterns.copy()
    
    def is_pattern_active(self, pattern: Pattern) -> bool:
        """Check if a specific pattern is currently active."""
        return pattern in self.active_patterns
    
    def update_pattern_performance(self, pattern_name: str, processing_time_ms: float, 
                                 matches_found: int = 0):
        """
        Update performance statistics for a pattern.
        
        Args:
            pattern_name: Name of the pattern
            processing_time_ms: Processing time in milliseconds
            matches_found: Number of matches found
        """
        if pattern_name not in self.pattern_performance_stats:
            self.pattern_performance_stats[pattern_name] = {
                'matches_found': 0,
                'avg_processing_time': 0.0,
                'last_active': datetime.now(),
                'activation_count': 1
            }
        
        stats = self.pattern_performance_stats[pattern_name]
        
        # Update average processing time using exponential moving average
        alpha = 0.1  # Smoothing factor
        stats['avg_processing_time'] = (
            (1 - alpha) * stats['avg_processing_time'] + alpha * processing_time_ms
        )
        
        # Update match count
        stats['matches_found'] += matches_found
        stats['last_active'] = datetime.now()
    
    def adjust_priorities_based_on_performance(self):
        """
        Dynamically adjust pattern priorities based on performance data.
        
        This method updates pattern priorities based on their effectiveness
        and resource consumption.
        """
        current_time = datetime.now()
        
        for pattern in self.all_patterns:
            pattern_name = self._get_pattern_name(pattern)
            
            if pattern_name not in self.pattern_performance_stats:
                continue
            
            stats = self.pattern_performance_stats[pattern_name]
            
            # Calculate performance score
            matches_per_activation = stats['matches_found'] / max(stats['activation_count'], 1)
            processing_efficiency = 1000.0 / max(stats['avg_processing_time'], 1.0)  # Inversely related to time
            
            # Combine metrics for new priority
            performance_score = (matches_per_activation * 0.7 + processing_efficiency * 0.3)
            
            # Update priority (bounded between 1 and 10)
            current_priority = self.pattern_priorities.get(pattern_name, 5.0)
            new_priority = min(10.0, max(1.0, current_priority * 0.9 + performance_score * 0.1))
            
            self.pattern_priorities[pattern_name] = new_priority
        
        # Re-sort patterns with updated priorities
        self._sorted_patterns = self._sort_patterns_by_priority()
    
    def get_adjustment_history(self) -> List[Dict]:
        """Get history of pattern adjustments."""
        return self.adjustment_history.copy()
    
    def get_pattern_statistics(self) -> Dict:
        """Get comprehensive pattern statistics."""
        stats = {}
        
        for pattern in self.all_patterns:
            pattern_name = self._get_pattern_name(pattern)
            
            perf_stats = self.pattern_performance_stats.get(pattern_name, {})
            
            stats[pattern_name] = {
                'priority': self.pattern_priorities.get(pattern_name, 0.0),
                'currently_active': pattern in self.active_patterns,
                'matches_found': perf_stats.get('matches_found', 0),
                'avg_processing_time_ms': perf_stats.get('avg_processing_time', 0.0),
                'activation_count': perf_stats.get('activation_count', 0),
                'last_active': perf_stats.get('last_active', datetime.now()).isoformat()
            }
        
        return stats
    
    def set_custom_load_ratios(self, load_ratios: Dict[LoadLevel, float]):
        """
        Set custom ratios for pattern activation at different load levels.
        
        Args:
            load_ratios: Dictionary mapping load levels to activation ratios (0.0-1.0)
        """
        for level, ratio in load_ratios.items():
            if not 0.0 <= ratio <= 1.0:
                raise ValueError(f"Load ratio for {level} must be between 0.0 and 1.0")
            self.load_level_ratios[level] = ratio
    
    def force_pattern_activation(self, pattern_names: List[str]):
        """
        Force activation of specific patterns regardless of load level.
        
        Args:
            pattern_names: List of pattern names to keep always active
        """
        forced_patterns = []
        for pattern in self.all_patterns:
            pattern_name = self._get_pattern_name(pattern)
            if pattern_name in pattern_names:
                forced_patterns.append(pattern)
        
        # Update active patterns to include forced ones
        self.active_patterns.update(forced_patterns)
    
    def get_load_level_summary(self) -> Dict[str, Dict]:
        """
        Get summary of what would happen at each load level.
        
        Returns:
            Dict mapping load levels to pattern activation info
        """
        summary = {}
        
        for load_level in LoadLevel:
            ratio = self.load_level_ratios.get(load_level, 1.0)
            target_count = max(1, int(len(self.all_patterns) * ratio))
            active_patterns = self._sorted_patterns[:target_count]
            
            summary[load_level.value] = {
                'activation_ratio': ratio,
                'patterns_active': target_count,
                'patterns_total': len(self.all_patterns),
                'active_pattern_names': [self._get_pattern_name(p) for p in active_patterns]
            }
        
        return summary
    
    def reset_statistics(self):
        """Reset all performance statistics and adjustment history."""
        self.adjustment_history.clear()
        
        for pattern_name in self.pattern_performance_stats:
            self.pattern_performance_stats[pattern_name] = {
                'matches_found': 0,
                'avg_processing_time': 0.0,
                'last_active': datetime.now(),
                'activation_count': 0
            }
        
        # Reset to all patterns active
        self.active_patterns = set(self.all_patterns)