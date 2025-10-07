"""
This file contains the main class of the project. It processes streams of events and detects pattern matches
by invoking the rest of the system components.
"""
from base.DataFormatter import DataFormatter
from parallel.EvaluationManagerFactory import EvaluationManagerFactory
from parallel.ParallelExecutionParameters import ParallelExecutionParameters
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
# Try alternative import paths for EvaluationMechanismParameters
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters

from typing import List, Optional
from datetime import datetime
from transformation.PatternPreprocessingParameters import PatternPreprocessingParameters
from transformation.PatternPreprocessor import PatternPreprocessor

# Load shedding imports
from loadshedding import (
    LoadSheddingConfig, LoadMonitor, LoadSheddingStrategy, 
    ProbabilisticLoadShedding, SemanticLoadShedding, AdaptiveLoadShedding, NoLoadShedding,
    LoadSheddingMetrics, LoadAwareInputStream, AdaptivePatternManager
)
LOAD_SHEDDING_AVAILABLE = True


class CEP:
    """
    A CEP object wraps the engine responsible for actual processing. It accepts the desired workload (list of patterns
    to be evaluated) and a set of settings defining the evaluation mechanism to be used and the way the workload should
    be optimized and parallelized.
    
    This enhanced version includes optional load shedding capabilities for handling high-volume event streams.
    """
    def __init__(self, patterns, eval_mechanism_params=None,
                 parallel_execution_params: ParallelExecutionParameters = None,
                 pattern_preprocessing_params: PatternPreprocessingParameters = None,
                 load_shedding_config: Optional['LoadSheddingConfig'] = None):
        """
        Constructor of the class.
        
        Args:
            patterns: Pattern or list of patterns to evaluate
            eval_mechanism_params: Parameters for evaluation mechanism
            parallel_execution_params: Parameters for parallel execution
            pattern_preprocessing_params: Parameters for pattern preprocessing
            load_shedding_config: Optional configuration for load shedding
        """
        actual_patterns = PatternPreprocessor(pattern_preprocessing_params).transform_patterns(patterns)
        self.__evaluation_manager = EvaluationManagerFactory.create_evaluation_manager(actual_patterns,
                                                                                       eval_mechanism_params,
                                                                                       parallel_execution_params)
        
        # Initialize load shedding components if available and configured
        self.__load_shedding_enabled = False
        self.__load_monitor = None
        self.__shedding_strategy = None
        self.__pattern_manager = None
        self.__metrics_collector = None
        
        if LOAD_SHEDDING_AVAILABLE and load_shedding_config and load_shedding_config.enabled:
            self._initialize_load_shedding(load_shedding_config, actual_patterns)

    def _initialize_load_shedding(self, config: 'LoadSheddingConfig', patterns: List[Pattern]):
        """Initialize load shedding components."""
        try:
            # Initialize load monitor
            self.__load_monitor = LoadMonitor(
                memory_threshold=config.memory_threshold,
                cpu_threshold=config.cpu_threshold,
                queue_threshold=config.queue_threshold,
                latency_threshold_ms=config.latency_threshold_ms,
                monitoring_interval_sec=config.monitoring_interval_sec
            )
            
            # Initialize load shedding strategy
            if config.strategy_name == 'probabilistic':
                self.__shedding_strategy = ProbabilisticLoadShedding(config.drop_probabilities)
            elif config.strategy_name == 'semantic':
                self.__shedding_strategy = SemanticLoadShedding(
                    config.pattern_priorities, 
                    config.importance_attributes
                )
            elif config.strategy_name == 'adaptive':
                self.__shedding_strategy = AdaptiveLoadShedding(config.adaptive_learning_rate)
            else:
                self.__shedding_strategy = NoLoadShedding()
            
            # Initialize pattern manager
            self.__pattern_manager = AdaptivePatternManager(patterns, config.pattern_priorities)
            
            # Initialize metrics collector
            self.__metrics_collector = LoadSheddingMetrics()
            
            self.__load_shedding_enabled = True
            print(f"Load shedding initialized with {config.strategy_name} strategy")
            
        except Exception as e:
            print(f"Error initializing load shedding: {e}")
            self.__load_shedding_enabled = False

    def run(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Applies the evaluation mechanism to detect the predefined patterns in a given stream of events.
        Returns the total time elapsed during evaluation.
        
        If load shedding is enabled, the input stream will be wrapped with load-aware capabilities.
        """
        start = datetime.now()
        
        # Wrap input stream with load shedding if enabled
        if self.__load_shedding_enabled:
            print("Enable load shedding for this CEP run")
            events = self._wrap_with_load_shedding(events)
            self.__metrics_collector.start_collection()
        
        # try:
        print("Starting evaluation...")
        print(f"Evaluation manager type: {type(self.__evaluation_manager)}")
        print(f"Events type: {type(events)}")
        print(f"Matches type: {type(matches)}")
        print(f"Data formatter type: {type(data_formatter)}")
        
        self.__evaluation_manager.eval(events, matches, data_formatter)
        print("Evaluation completed!")
        # finally:
        #     if self.__load_shedding_enabled:
        #         self.__metrics_collector.stop_collection()
        
        return (datetime.now() - start).total_seconds()
    
    def _wrap_with_load_shedding(self, events: InputStream) -> InputStream:
        """Wrap the input stream with load shedding capabilities."""
        return LoadAwareInputStream(
            events, 
            self.__load_monitor, 
            self.__shedding_strategy,
            self.__metrics_collector
        )

    def get_pattern_match(self):
        """
        Returns one match from the output stream.
        """
        # try:
        return self.get_pattern_match_stream().get_item()
        # except StopIteration:  # the stream might be closed.
            # return None

    def get_pattern_match_stream(self):
        """
        Returns the output stream containing the detected matches.
        """
        return self.__evaluation_manager.get_pattern_match_stream()

    def get_evaluation_mechanism_structure_summary(self):
        """
        Returns an object summarizing the structure of the underlying evaluation mechanism.
        """
        return self.__evaluation_manager.get_structure_summary()
    
    def get_load_shedding_metrics(self) -> Optional['LoadSheddingMetrics']:
        """
        Get load shedding metrics if load shedding is enabled.
        
        Returns:
            LoadSheddingMetrics object or None if load shedding is disabled
        """
        if self.__load_shedding_enabled:
            return self.__metrics_collector
        return None
    
    def get_load_shedding_statistics(self) -> Optional[dict]:
        """
        Get current load shedding statistics.
        
        Returns:
            Dictionary with load shedding statistics or None if disabled
        """
        if not self.__load_shedding_enabled:
            return None
        
        stats = {
            'enabled': True,
            'strategy': self.__shedding_strategy.name if self.__shedding_strategy else 'Unknown',
            'current_load_level': None,
            'drop_rate': 0.0,
            'events_processed': 0,
            'events_dropped': 0
        }
        
        if self.__metrics_collector:
            stats.update({
                'drop_rate': self.__metrics_collector.get_current_drop_rate(),
                'events_processed': self.__metrics_collector.events_processed,
                'events_dropped': self.__metrics_collector.events_dropped,
                'patterns_matched': self.__metrics_collector.patterns_matched,
                'avg_throughput_eps': self.__metrics_collector.get_average_throughput(),
                'avg_latency_ms': self.__metrics_collector.get_average_latency()
            })
        
        if self.__load_monitor:
            current_metrics = self.__load_monitor.get_current_metrics()
            stats['current_load_level'] = current_metrics.get('load_level', 'unknown')
        
        return stats
    
    def is_load_shedding_enabled(self) -> bool:
        """Check if load shedding is enabled for this CEP instance."""
        return self.__load_shedding_enabled
    
    def update_load_shedding_config(self, new_config: 'LoadSheddingConfig'):
        """
        Update load shedding configuration at runtime.
        
        Args:
            new_config: New load shedding configuration
        """
        if not LOAD_SHEDDING_AVAILABLE:
            print("Load shedding module not available, cannot update configuration")
            return
        
        if new_config.enabled and not self.__load_shedding_enabled:
            # Enable load shedding
            patterns = []  # We'd need to store patterns for this to work properly
            self._initialize_load_shedding(new_config, patterns)
        elif not new_config.enabled and self.__load_shedding_enabled:
            # Disable load shedding
            self.__load_shedding_enabled = False
            print("Load shedding disabled")
        elif self.__load_shedding_enabled:
            # Update existing configuration
            try:
                # Update thresholds
                if self.__load_monitor:
                    self.__load_monitor.memory_threshold = new_config.memory_threshold
                    self.__load_monitor.cpu_threshold = new_config.cpu_threshold
                    self.__load_monitor.queue_threshold = new_config.queue_threshold
                    self.__load_monitor.latency_threshold_ms = new_config.latency_threshold_ms
                
                # Update strategy if needed
                if new_config.strategy_name != self.__shedding_strategy.name:
                    if new_config.strategy_name == 'probabilistic':
                        self.__shedding_strategy = ProbabilisticLoadShedding(new_config.drop_probabilities)
                    elif new_config.strategy_name == 'semantic':
                        self.__shedding_strategy = SemanticLoadShedding(
                            new_config.pattern_priorities, 
                            new_config.importance_attributes
                        )
                    elif new_config.strategy_name == 'adaptive':
                        self.__shedding_strategy = AdaptiveLoadShedding(new_config.adaptive_learning_rate)
                    else:
                        self.__shedding_strategy = NoLoadShedding()
                
                print(f"Load shedding configuration updated to {new_config.strategy_name} strategy")
                
            except Exception as e:
                print(f"Error updating load shedding configuration: {e}")
