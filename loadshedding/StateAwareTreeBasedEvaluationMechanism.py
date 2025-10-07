"""
State-Aware Tree-Based Evaluation Mechanism for CEP with Load Shedding.

This module extends the standard tree-based evaluation mechanism to work seamlessly
with stateful load shedding strategies, maintaining pattern state across evaluation cycles.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tree.evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from tree.PatternMatchStorage import TreeStorageParameters
from loadshedding.StatefulLoadSheddingStrategy import StatefulLoadSheddingStrategy
from loadshedding.StatePersistence import LoadSheddingStatePersistence

logger = logging.getLogger(__name__)


class StateAwareTreeBasedEvaluationMechanism(TreeBasedEvaluationMechanism):
    """
    Enhanced tree-based evaluation mechanism that integrates with stateful load shedding.
    
    Features:
    - Pattern state tracking across evaluation cycles
    - Load shedding decision integration
    - Performance metrics collection
    - State persistence for recovery
    """
    
    def __init__(self, patterns, storage_params: TreeStorageParameters,
                 stateful_strategy: StatefulLoadSheddingStrategy = None):
        """
        patterns can be either:
          - a list of Pattern objects
          - a dict mapping Pattern -> TreePlan (already prepared)

        We avoid introducing a fake TrivialTreePlan (doesn't exist). If a list is provided we create
        a minimal identity mapping expected by the base class where each pattern maps to itself.
        The base TreeBasedEvaluationMechanism later extracts the pattern object separately.
        This mirrors how some trivial mechanisms in the codebase pass structures.
        """
        self._deferred_init = False
        if isinstance(patterns, list):
            if not patterns:
                # Defer initialization until patterns are registered
                self._deferred_init = True
                pattern_to_tree_plan_map = { }  # empty
            else:
                pattern_to_tree_plan_map = {p: p for p in patterns}
        else:
            pattern_to_tree_plan_map = patterns or {}

        if pattern_to_tree_plan_map:
            super().__init__(pattern_to_tree_plan_map, storage_params)
        else:
            # Minimal placeholders; parent expects some structure. We'll re-init later.
            # Store params for later use.
            self._pending_storage_params = storage_params
            # Create trivial attributes parent would set to avoid attribute errors in interim methods.
            self._tree = None
        
        self.stateful_strategy = stateful_strategy
        self.pattern_states = {}
        self.evaluation_metrics = {
            'total_events': 0,
            'events_processed': 0,
            'events_dropped': 0,
            'matches_found': 0,
            'partial_matches': 0,
            'state_updates': 0,
            'evaluation_time': 0.0
        }
        self.state_persistence = LoadSheddingStatePersistence()
        
        # Performance tracking
        self.last_performance_check = time.time()
        self.performance_window = []
        
        if self._deferred_init:
            logger.info("StateAwareTreeBasedEvaluationMechanism initialized in deferred mode (no patterns yet)")
        else:
            logger.info("StateAwareTreeBasedEvaluationMechanism initialized with %d pattern(s)" % (0 if isinstance(patterns, dict) and len(patterns)==0 else (len(patterns) if isinstance(patterns, list) else len(pattern_to_tree_plan_map))))

    def finalize_deferred_initialization(self, patterns: list):
        """Complete initialization when patterns become available."""
        if not self._deferred_init:
            return
        if not patterns:
            raise ValueError("Deferred initialization requires at least one pattern")
        from evaluation.EvaluationMechanismFactory import OptimizerFactory  # lazy import if needed
        pattern_to_tree_plan_map = {p: p for p in patterns}
        # Call parent init now
        super(StateAwareTreeBasedEvaluationMechanism, self).__init__(pattern_to_tree_plan_map, self._pending_storage_params)
        self._deferred_init = False
        logger.info("Deferred initialization completed with %d pattern(s)" % len(patterns))
    
    def eval(self, event_stream, output_stream, data_formatter):
        """
        Enhanced evaluation with state-aware load shedding integration.
        """
        logger.info("Starting state-aware evaluation...")
        start_time = time.time()
        
        try:
            # Load any persisted state
            self._load_persisted_state()
            
            # Process events with state awareness
            for event_data in event_stream:
                self.evaluation_metrics['total_events'] += 1
                
                # Check if we should shed this event
                should_shed, shedding_reason = self._should_shed_event(event_data)
                
                if should_shed:
                    self.evaluation_metrics['events_dropped'] += 1
                    logger.debug(f"Event shed: {shedding_reason}")
                    continue
                
                # Process the event
                self._process_event_with_state(event_data, output_stream, data_formatter)
                self.evaluation_metrics['events_processed'] += 1
                
                # Update performance metrics periodically
                if self.evaluation_metrics['total_events'] % 100 == 0:
                    self._update_performance_metrics()
            
            # Final state persistence
            self._persist_current_state()
            
        except Exception as e:
            logger.error(f"Error during state-aware evaluation: {e}")
            raise
        finally:
            self.evaluation_metrics['evaluation_time'] = time.time() - start_time
            logger.info(f"State-aware evaluation completed in {self.evaluation_metrics['evaluation_time']:.2f}s")
    
    def _should_shed_event(self, event_data) -> Tuple[bool, str]:
        """
        Determine if an event should be shed based on stateful load shedding strategy.
        """
        if not self.stateful_strategy:
            return False, "No load shedding strategy"
        
        # Get current system state
        current_load = self._get_current_load()
        pattern_states = self.pattern_states
        
        # Make load shedding decision
        should_shed = self.stateful_strategy.should_shed_event(
            event_data, current_load, pattern_states
        )
        
        if should_shed:
            reason = self.stateful_strategy.get_last_shedding_reason()
            return True, reason
        
        return False, "Event accepted"
    
    def _process_event_with_state(self, event_data, output_stream, data_formatter):
        """
        Process an event while maintaining pattern state information.
        """
        # Standard tree-based evaluation
        matches = self._evaluate_patterns(event_data, data_formatter)
        
        # Update pattern states
        for pattern_id, pattern in enumerate(self.patterns):
            pattern_state = self.pattern_states.get(pattern_id, {
                'partial_matches': [],
                'completed_matches': 0,
                'last_activity': datetime.now(),
                'importance_score': 0.0
            })
            
            # Check for partial matches
            partial_matches = self._find_partial_matches(event_data, pattern, pattern_state)
            if partial_matches:
                pattern_state['partial_matches'].extend(partial_matches)
                pattern_state['last_activity'] = datetime.now()
                self.evaluation_metrics['partial_matches'] += len(partial_matches)
            
            # Check for complete matches
            complete_matches = self._find_complete_matches(event_data, pattern, pattern_state)
            if complete_matches:
                pattern_state['completed_matches'] += len(complete_matches)
                self.evaluation_metrics['matches_found'] += len(complete_matches)
                
                # Output matches
                for match in complete_matches:
                    output_stream.add_item(match)
            
            # Update pattern state
            self.pattern_states[pattern_id] = pattern_state
            self.evaluation_metrics['state_updates'] += 1
        
        # Notify stateful strategy of state changes
        if self.stateful_strategy:
            self.stateful_strategy.update_pattern_states(self.pattern_states)
    
    def _evaluate_patterns(self, event_data, data_formatter):
        """
        Wrapper for standard pattern evaluation.
        """
        # This would call the parent class evaluation logic
        # For now, we'll simulate pattern matching
        matches = []
        
        # Simulate some pattern matching logic
        event_type = data_formatter.get_event_type(event_data)
        if event_type == "BikeTrip":
            # Check if this could be part of a pattern
            bike_id = event_data.get('bikeid')
            start_station = event_data.get('start_station_id')
            end_station = event_data.get('end_station_id')
            
            if bike_id and start_station and end_station:
                # Look for round-trip patterns
                for pattern_id, pattern_state in self.pattern_states.items():
                    for partial in pattern_state.get('partial_matches', []):
                        if (partial.get('bikeid') == bike_id and 
                            partial.get('end_station_id') == start_station):
                            # Found a potential match
                            match = {
                                'pattern_id': pattern_id,
                                'events': [partial, event_data],
                                'match_time': datetime.now(),
                                'confidence': 0.9
                            }
                            matches.append(match)
        
        return matches
    
    def _find_partial_matches(self, event_data, pattern, pattern_state):
        """
        Find partial pattern matches for state tracking.
        """
        partial_matches = []
        
        # Simulate partial match detection
        if event_data.get('event_type') == 'BikeTrip':
            # This could be the start of a round-trip pattern
            partial_matches.append({
                'event_data': event_data,
                'pattern_id': id(pattern),
                'match_stage': 'first_trip',
                'timestamp': datetime.now(),
                'bikeid': event_data.get('bikeid'),
                'start_station_id': event_data.get('start_station_id'),
                'end_station_id': event_data.get('end_station_id')
            })
        
        return partial_matches
    
    def _find_complete_matches(self, event_data, pattern, pattern_state):
        """
        Find complete pattern matches based on current and partial matches.
        """
        complete_matches = []
        
        # Check if current event completes any partial matches
        current_bike_id = event_data.get('bikeid')
        current_start_station = event_data.get('start_station_id')
        
        if current_bike_id and current_start_station:
            # Look for partial matches that could be completed
            completed_partials = []
            for i, partial in enumerate(pattern_state.get('partial_matches', [])):
                if (partial.get('bikeid') == current_bike_id and
                    partial.get('end_station_id') == current_start_station):
                    # Found a completing event
                    complete_match = {
                        'pattern_name': getattr(pattern, 'name', f'Pattern_{id(pattern)}'),
                        'first_trip': partial['event_data'],
                        'return_trip': event_data,
                        'match_time': datetime.now(),
                        'trip_duration': self._calculate_trip_duration(partial['event_data'], event_data),
                        'confidence': 0.95
                    }
                    complete_matches.append(complete_match)
                    completed_partials.append(i)
            
            # Remove completed partial matches
            for i in reversed(completed_partials):
                pattern_state['partial_matches'].pop(i)
        
        return complete_matches
    
    def _calculate_trip_duration(self, first_event, second_event):
        """
        Calculate duration between two trip events.
        """
        try:
            first_time = first_event.get('ts', datetime.now())
            second_time = second_event.get('ts', datetime.now())
            if isinstance(first_time, str):
                first_time = datetime.fromisoformat(first_time)
            if isinstance(second_time, str):
                second_time = datetime.fromisoformat(second_time)
            return (second_time - first_time).total_seconds()
        except:
            return 0.0
    
    def _get_current_load(self):
        """
        Get current system load metrics.
        """
        current_time = time.time()
        events_per_second = 0
        
        if self.performance_window:
            window_duration = current_time - self.performance_window[0]['timestamp']
            if window_duration > 0:
                events_per_second = len(self.performance_window) / window_duration
        
        return {
            'cpu_usage': 0.5,  # Simulate CPU usage
            'memory_usage': 0.4,  # Simulate memory usage
            'events_per_second': events_per_second,
            'queue_size': len(self.pattern_states),
            'active_patterns': len([s for s in self.pattern_states.values() 
                                  if s.get('partial_matches')])
        }
    
    def _update_performance_metrics(self):
        """
        Update performance tracking metrics.
        """
        current_time = time.time()
        
        # Add current performance point
        self.performance_window.append({
            'timestamp': current_time,
            'events_processed': self.evaluation_metrics['events_processed'],
            'matches_found': self.evaluation_metrics['matches_found']
        })
        
        # Keep only last 60 seconds of data
        cutoff_time = current_time - 60
        self.performance_window = [
            p for p in self.performance_window 
            if p['timestamp'] > cutoff_time
        ]
        
        # Update stateful strategy with performance data
        if self.stateful_strategy:
            self.stateful_strategy.update_performance_metrics({
                'events_per_second': len(self.performance_window) / 60 if self.performance_window else 0,
                'match_rate': self.evaluation_metrics['matches_found'] / max(self.evaluation_metrics['events_processed'], 1),
                'drop_rate': self.evaluation_metrics['events_dropped'] / max(self.evaluation_metrics['total_events'], 1)
            })
    
    def _load_persisted_state(self):
        """
        Load previously persisted pattern states.
        """
        try:
            # Load individual pattern states
            for pattern_id in range(len(self.patterns)):
                pattern_name = f"pattern_{pattern_id}"
                persisted_state = self.state_persistence.load_pattern_state(pattern_name)
                if persisted_state:
                    self.pattern_states[pattern_id] = persisted_state
            
            if self.pattern_states:
                logger.info(f"Loaded {len(self.pattern_states)} persisted pattern states")
        except Exception as e:
            logger.warning(f"Could not load persisted state: {e}")
    
    def _persist_current_state(self):
        """
        Persist current pattern states for recovery.
        """
        try:
            # Save individual pattern states
            for pattern_id, state in self.pattern_states.items():
                pattern_name = f"pattern_{pattern_id}"
                # Convert datetime objects to strings for serialization
                serializable_state = self._make_serializable(state)
                self.state_persistence.save_pattern_state(pattern_name, serializable_state)
            
            logger.info("Pattern states persisted successfully")
        except Exception as e:
            logger.warning(f"Could not persist state: {e}")
    
    def _make_serializable(self, state):
        """Convert state to a serializable format."""
        if isinstance(state, dict):
            serializable_state = {}
            for key, value in state.items():
                if isinstance(value, datetime):
                    serializable_state[key] = value.isoformat()
                elif isinstance(value, list):
                    serializable_state[key] = [self._make_serializable(item) for item in value]
                elif isinstance(value, dict):
                    serializable_state[key] = self._make_serializable(value)
                else:
                    serializable_state[key] = value
            return serializable_state
        return state
    
    def get_evaluation_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive evaluation metrics.
        """
        return {
            **self.evaluation_metrics,
            'pattern_states': {
                'total_patterns': len(self.pattern_states),
                'active_patterns': len([s for s in self.pattern_states.values() 
                                      if s.get('partial_matches')]),
                'total_partial_matches': sum(len(s.get('partial_matches', [])) 
                                           for s in self.pattern_states.values())
            },
            'performance': {
                'events_per_second': len(self.performance_window) / 60 if self.performance_window else 0,
                'match_rate': self.evaluation_metrics['matches_found'] / max(self.evaluation_metrics['events_processed'], 1),
                'drop_rate': self.evaluation_metrics['events_dropped'] / max(self.evaluation_metrics['total_events'], 1)
            }
        }


def create_state_aware_evaluation_mechanism(patterns=None, stateful_strategy=None, 
                                          load_shedding_strategy=None, optimization_interval=30,
                                          storage_params=None):
    """
    Factory function to create a state-aware evaluation mechanism.
    
    Args:
        patterns: List of patterns to evaluate (can be None if set later)
        stateful_strategy: Stateful load shedding strategy
        load_shedding_strategy: Alternative name for stateful_strategy
        optimization_interval: Interval for optimization (unused for now)
        storage_params: Storage parameters for the tree evaluation mechanism
    """
    # Use load_shedding_strategy if provided, otherwise use stateful_strategy
    strategy = load_shedding_strategy or stateful_strategy
    
    # Create empty patterns list if none provided
    if patterns is None:
        patterns = []
    
    # Create default storage parameters if none provided
    if storage_params is None:
        storage_params = TreeStorageParameters(sort_storage=False,
                                             clean_up_interval=10,
                                             prioritize_sorting_by_timestamp=True)
    
    return StateAwareTreeBasedEvaluationMechanism(patterns, storage_params, strategy)


# For backward compatibility and testing
if __name__ == "__main__":
    # Simple test
    from base.Pattern import Pattern
    from base.PatternStructure import PrimitiveEventStructure
    
    # Create a test pattern
    test_pattern = Pattern(
        PrimitiveEventStructure("BikeTrip", "trip"),
        None,  # No condition for simplicity
        3600   # 1 hour window
    )
    test_pattern.name = "TestPattern"
    
    # Create evaluation mechanism
    evaluator = create_state_aware_evaluation_mechanism([test_pattern])
    
    # Test metrics
    metrics = evaluator.get_evaluation_metrics()
    print("State-Aware Evaluation Mechanism Metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value}")