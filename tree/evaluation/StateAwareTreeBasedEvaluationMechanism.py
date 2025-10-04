"""
State-Aware Tree Based Evaluation Mechanism.
This module extends the TreeBasedEvaluationMechanism with stateful load shedding
and pattern completion state management.
"""

from typing import Dict, Optional, List
from datetime import timedelta
from base.Pattern import Pattern
from base.Event import Event
from base.DataFormatter import DataFormatter
from plan.TreePlan import TreePlan
from stream.Stream import InputStream, OutputStream
from tree.PatternMatchStorage import TreeStorageParameters
from tree.evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from adaptive.statistics import StatisticsCollector
from adaptive.optimizer import Optimizer
from loadshedding.StatefulLoadSheddingStrategy import StatefulLoadSheddingStrategy, PatternStateManager
from loadshedding.LoadMonitor import LoadMonitor, LoadLevel
import time
import logging

logger = logging.getLogger(__name__)


class StateAwareTreeBasedEvaluationMechanism(TreeBasedEvaluationMechanism):
    """
    Enhanced TreeBasedEvaluationMechanism with integrated stateful load shedding
    and pattern state management.
    """
    
    def __init__(self, 
                 pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 storage_params: TreeStorageParameters,
                 statistics_collector: StatisticsCollector = None,
                 optimizer: Optimizer = None,
                 statistics_update_time_window: timedelta = None,
                 load_monitor: LoadMonitor = None,
                 stateful_load_shedding: StatefulLoadSheddingStrategy = None):
        """
        Initialize state-aware evaluation mechanism.
        
        Args:
            pattern_to_tree_plan_map: Mapping of patterns to tree plans
            storage_params: Tree storage parameters
            statistics_collector: Statistics collector for optimization
            optimizer: Tree optimizer
            statistics_update_time_window: Window for statistics updates
            load_monitor: Load monitoring system
            stateful_load_shedding: Stateful load shedding strategy
        """
        super().__init__(pattern_to_tree_plan_map, storage_params, 
                        statistics_collector, optimizer, statistics_update_time_window)
        
        # State management components
        self.load_monitor = load_monitor
        self.stateful_load_shedding = stateful_load_shedding
        self.pattern_state_manager = PatternStateManager() if stateful_load_shedding else None
        
        # Performance tracking
        self.state_based_optimizations = 0
        self.load_shedding_decisions = 0
        self.pattern_completion_tracking = {}
        
        # Initialize pattern tracking
        self._initialize_pattern_tracking(pattern_to_tree_plan_map)
    
    def _initialize_pattern_tracking(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan]):
        """Initialize tracking for each pattern in the system."""
        for pattern in pattern_to_tree_plan_map.keys():
            pattern_name = getattr(pattern, 'name', str(pattern))
            self.pattern_completion_tracking[pattern_name] = {
                'partial_matches': {},
                'completion_history': [],
                'avg_completion_time': 0.0,
                'success_rate': 0.0
            }
    
    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Enhanced evaluation with stateful load shedding integration.
        
        Args:
            events: Input event stream
            matches: Output match stream
            data_formatter: Data formatter for events
        """
        logger.info("Starting state-aware evaluation...")
        
        # Initialize state tracking
        last_statistics_refresh_time = None
        event_count = 0
        state_decisions = 0
        
        try:
            for event in events:
                event_count += 1
                current_time = time.time()
                
                # Monitor system load if available
                current_load_level = LoadLevel.NORMAL
                if self.load_monitor:
                    current_load_level = self.load_monitor.assess_load_level()
                
                # Apply stateful load shedding decision
                should_drop = False
                if self.stateful_load_shedding and current_load_level != LoadLevel.NORMAL:
                    should_drop = self.stateful_load_shedding.should_drop_event(event, current_load_level)
                    if should_drop:
                        self.load_shedding_decisions += 1
                        continue  # Drop this event
                
                # Track event for pattern completion analysis
                if self.pattern_state_manager:
                    self._track_event_for_patterns(event)
                
                # Process event through normal tree evaluation
                if self._should_try_reoptimize(last_statistics_refresh_time, event):
                    last_statistics_refresh_time = self.__perform_reoptimization(
                        last_statistics_refresh_time, event)
                
                # Let the tree handle the event
                self._play_new_event(event, self._event_types_listeners)
                
                # Collect matches and update state
                new_matches = self._get_matches(matches)
                if new_matches:
                    self._update_pattern_completion_state(new_matches)
                
                # Periodic state-based optimization
                if event_count % 1000 == 0:
                    self._perform_state_based_optimization()
                    state_decisions += 1
            
            # Final match collection
            final_matches = self._get_last_pending_matches(matches)
            if final_matches:
                self._update_pattern_completion_state(final_matches)
            
            logger.info(f"State-aware evaluation completed. Events: {event_count}, "
                       f"Load shedding decisions: {self.load_shedding_decisions}, "
                       f"State optimizations: {state_decisions}")
        
        except Exception as e:
            logger.error(f"Error in state-aware evaluation: {e}")
            raise
    
    def _track_event_for_patterns(self, event: Event):
        """Track event for potential pattern matches."""
        if not self.pattern_state_manager:
            return
        
        # Analyze which patterns this event might contribute to
        potential_patterns = self._identify_potential_patterns(event)
        
        for pattern_name in potential_patterns:
            # Check if this event could start a new partial match
            if self._could_start_partial_match(event, pattern_name):
                match_id = self.pattern_state_manager.start_partial_match(pattern_name, event)
                logger.debug(f"Started partial match {match_id} for pattern {pattern_name}")
            
            # Check if this event could extend existing partial matches
            existing_matches = self._find_extendable_matches(event, pattern_name)
            for match_id in existing_matches:
                self.pattern_state_manager.update_partial_match(match_id, event)
                logger.debug(f"Extended partial match {match_id} with event {event.id}")
    
    def _identify_potential_patterns(self, event: Event) -> List[str]:
        """Identify which patterns this event might contribute to."""
        potential_patterns = []
        
        # Simple heuristic based on event type
        event_type = getattr(event, 'event_type', 'unknown')
        
        for pattern_name in self.pattern_completion_tracking.keys():
            # This is a simplified check - in practice, you'd want more sophisticated logic
            if event_type in pattern_name or 'default' in pattern_name.lower():
                potential_patterns.append(pattern_name)
        
        return potential_patterns
    
    def _could_start_partial_match(self, event: Event, pattern_name: str) -> bool:
        """Determine if this event could start a new partial match."""
        # Simplified logic - could be enhanced with pattern structure analysis
        return True  # For now, assume any event could potentially start a match
    
    def _find_extendable_matches(self, event: Event, pattern_name: str) -> List[str]:
        """Find existing partial matches that could be extended by this event."""
        if not self.pattern_state_manager:
            return []
        
        extendable_matches = []
        
        # Check existing partial matches for this pattern
        for match_id, partial_match in self.pattern_state_manager.partial_matches.items():
            if (partial_match.pattern_name == pattern_name and 
                not partial_match.is_expired(time.time())):
                # Simple temporal proximity check
                if abs(event.timestamp - partial_match.last_update) < 300:  # 5 minutes
                    extendable_matches.append(match_id)
        
        return extendable_matches
    
    def _update_pattern_completion_state(self, matches):
        """Update pattern completion state when matches are found."""
        if not self.pattern_state_manager:
            return
        
        for match in matches:
            pattern_name = getattr(match, 'pattern_name', 'unknown')
            
            # Find corresponding partial match and mark as completed
            for match_id, partial_match in list(self.pattern_state_manager.partial_matches.items()):
                if partial_match.pattern_name == pattern_name:
                    self.pattern_state_manager.complete_partial_match(match_id, match)
                    logger.debug(f"Completed partial match {match_id} for pattern {pattern_name}")
                    break
    
    def _perform_state_based_optimization(self):
        """Perform optimization based on current state information."""
        if not self.pattern_state_manager:
            return
        
        self.state_based_optimizations += 1
        
        # Get current state summary
        state_summary = self.pattern_state_manager.get_state_summary()
        
        # Analyze pattern performance and adjust priorities
        for pattern_name, stats in state_summary.get('pattern_statistics', {}).items():
            completion_rate = stats.get('completion_rate', 0.5)
            
            # If a pattern has very low completion rate, consider deprioritizing it
            if completion_rate < 0.1 and stats.get('total_attempts', 0) > 100:
                logger.info(f"Pattern {pattern_name} has low completion rate ({completion_rate:.2%}), "
                           "consider optimization")
                self._suggest_pattern_optimization(pattern_name, stats)
    
    def _suggest_pattern_optimization(self, pattern_name: str, stats: Dict):
        """Suggest optimizations for poorly performing patterns."""
        # This could trigger tree restructuring, parameter adjustment, etc.
        logger.info(f"Optimization suggestion for {pattern_name}: "
                   f"completion_rate={stats.get('completion_rate', 0):.2%}, "
                   f"total_attempts={stats.get('total_attempts', 0)}")
        
        # Example: Adjust pattern timeouts or selectivity
        if self.stateful_load_shedding:
            current_priority = self.stateful_load_shedding.pattern_priorities.get(pattern_name, 1.0)
            new_priority = max(0.1, current_priority * 0.8)  # Reduce priority
            self.stateful_load_shedding.pattern_priorities[pattern_name] = new_priority
            logger.info(f"Reduced priority for {pattern_name}: {current_priority} -> {new_priority}")
    
    def get_state_statistics(self) -> Dict:
        """Get comprehensive state statistics."""
        base_stats = {
            'state_based_optimizations': self.state_based_optimizations,
            'load_shedding_decisions': self.load_shedding_decisions,
            'pattern_completion_tracking': self.pattern_completion_tracking
        }
        
        if self.stateful_load_shedding:
            base_stats['load_shedding_stats'] = self.stateful_load_shedding.get_state_statistics()
        
        if self.pattern_state_manager:
            base_stats['pattern_state_summary'] = self.pattern_state_manager.get_state_summary()
        
        return base_stats
    
    def reset_state(self):
        """Reset all state tracking."""
        self.state_based_optimizations = 0
        self.load_shedding_decisions = 0
        self.pattern_completion_tracking = {}
        
        if self.stateful_load_shedding:
            self.stateful_load_shedding.reset_statistics()
        
        if self.pattern_state_manager:
            self.pattern_state_manager = PatternStateManager()


class StateAwareEvaluationMechanismFactory:
    """Factory for creating state-aware evaluation mechanisms."""
    
    @staticmethod
    def create_state_aware_mechanism(
            pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
            storage_params: TreeStorageParameters,
            load_shedding_config: Optional[Dict] = None,
            **kwargs) -> StateAwareTreeBasedEvaluationMechanism:
        """
        Create a state-aware evaluation mechanism with optional load shedding.
        
        Args:
            pattern_to_tree_plan_map: Pattern to tree plan mapping
            storage_params: Storage parameters
            load_shedding_config: Load shedding configuration
            **kwargs: Additional arguments for evaluation mechanism
            
        Returns:
            StateAwareTreeBasedEvaluationMechanism: Configured mechanism
        """
        # Initialize load monitoring if requested
        load_monitor = None
        stateful_load_shedding = None
        
        if load_shedding_config and load_shedding_config.get('enabled', False):
            from loadshedding.LoadMonitor import LoadMonitor
            
            load_monitor = LoadMonitor(
                memory_threshold=load_shedding_config.get('memory_threshold', 0.8),
                cpu_threshold=load_shedding_config.get('cpu_threshold', 0.9),
                queue_threshold=load_shedding_config.get('queue_threshold', 1000)
            )
            
            # Create stateful load shedding strategy
            pattern_priorities = {}
            for pattern in pattern_to_tree_plan_map.keys():
                pattern_name = getattr(pattern, 'name', str(pattern))
                pattern_priorities[pattern_name] = load_shedding_config.get('pattern_priorities', {}).get(
                    pattern_name, 1.0)
            
            stateful_load_shedding = StatefulLoadSheddingStrategy(
                pattern_priorities=pattern_priorities,
                completion_threshold=load_shedding_config.get('completion_threshold', 0.3)
            )
        
        return StateAwareTreeBasedEvaluationMechanism(
            pattern_to_tree_plan_map=pattern_to_tree_plan_map,
            storage_params=storage_params,
            load_monitor=load_monitor,
            stateful_load_shedding=stateful_load_shedding,
            **kwargs
        )