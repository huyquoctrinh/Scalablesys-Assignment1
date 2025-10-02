"""
Benchmarking framework for load shedding strategies.
This module provides comprehensive testing and evaluation of different load shedding approaches.
"""

import time
import random
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import threading

from .LoadSheddingStrategy import LoadSheddingStrategy, NoLoadShedding
from .LoadSheddingMetrics import LoadSheddingMetrics
from .LoadMonitor import LoadMonitor, LoadLevel
from base.Event import Event
from base.Pattern import Pattern


class SyntheticWorkloadGenerator:
    """
    Generates synthetic workloads for load shedding testing.
    """
    
    def __init__(self, base_rate_eps: float = 1000):
        """
        Initialize workload generator.
        
        Args:
            base_rate_eps: Base event rate in events per second
        """
        self.base_rate_eps = base_rate_eps
        self.workload_patterns = {
            'constant_low': self._constant_load_pattern,
            'constant_high': self._constant_high_load_pattern,
            'bursty': self._bursty_load_pattern,
            'gradual_increase': self._gradual_increase_pattern,
            'spike_pattern': self._spike_pattern,
            'periodic': self._periodic_pattern
        }
    
    def generate_workload(self, pattern_name: str, duration_seconds: int) -> List[Event]:
        """
        Generate a synthetic workload based on the specified pattern.
        
        Args:
            pattern_name: Name of the workload pattern
            duration_seconds: Duration of the workload in seconds
            
        Returns:
            List[Event]: Generated events
        """
        if pattern_name not in self.workload_patterns:
            raise ValueError(f"Unknown workload pattern: {pattern_name}")
        
        generator_func = self.workload_patterns[pattern_name]
        return generator_func(duration_seconds)
    
    def _constant_load_pattern(self, duration: int) -> List[Event]:
        """Generate constant load at base rate."""
        total_events = int(self.base_rate_eps * duration)
        events = []
        
        for i in range(total_events):
            timestamp = i / self.base_rate_eps
            event = self._create_synthetic_event(f"event_{i}", timestamp)
            events.append(event)
        
        return events
    
    def _constant_high_load_pattern(self, duration: int) -> List[Event]:
        """Generate constant high load (3x base rate)."""
        high_rate = self.base_rate_eps * 3
        total_events = int(high_rate * duration)
        events = []
        
        for i in range(total_events):
            timestamp = i / high_rate
            event = self._create_synthetic_event(f"high_event_{i}", timestamp)
            events.append(event)
        
        return events
    
    def _bursty_load_pattern(self, duration: int) -> List[Event]:
        """Generate bursty load pattern with periods of high and low activity."""
        events = []
        current_time = 0
        event_id = 0
        
        while current_time < duration:
            # Random burst parameters
            burst_duration = random.uniform(0.5, 2.0)  # 0.5-2 second bursts
            burst_rate = self.base_rate_eps * random.uniform(2, 5)  # 2-5x normal rate
            quiet_duration = random.uniform(1.0, 3.0)  # 1-3 second quiet periods
            quiet_rate = self.base_rate_eps * random.uniform(0.1, 0.5)  # 10-50% normal rate
            
            # Generate burst
            burst_events = int(burst_rate * burst_duration)
            for i in range(burst_events):
                if current_time >= duration:
                    break
                event = self._create_synthetic_event(f"burst_event_{event_id}", current_time)
                events.append(event)
                current_time += 1.0 / burst_rate
                event_id += 1
            
            # Generate quiet period
            quiet_events = int(quiet_rate * quiet_duration)
            for i in range(quiet_events):
                if current_time >= duration:
                    break
                event = self._create_synthetic_event(f"quiet_event_{event_id}", current_time)
                events.append(event)
                current_time += 1.0 / quiet_rate
                event_id += 1
        
        return events
    
    def _gradual_increase_pattern(self, duration: int) -> List[Event]:
        """Generate gradually increasing load."""
        events = []
        current_time = 0
        event_id = 0
        
        while current_time < duration:
            # Rate increases linearly over time
            progress = current_time / duration
            current_rate = self.base_rate_eps * (1 + progress * 4)  # 1x to 5x over duration
            
            event = self._create_synthetic_event(f"increasing_event_{event_id}", current_time)
            events.append(event)
            
            current_time += 1.0 / current_rate
            event_id += 1
        
        return events
    
    def _spike_pattern(self, duration: int) -> List[Event]:
        """Generate pattern with sudden spikes."""
        events = []
        current_time = 0
        event_id = 0
        
        # Normal load most of the time
        normal_rate = self.base_rate_eps
        
        # Add random spikes
        spike_times = sorted([random.uniform(0, duration) for _ in range(3)])
        
        while current_time < duration:
            # Check if we're in a spike
            in_spike = any(abs(current_time - spike_time) < 0.5 for spike_time in spike_times)
            
            if in_spike:
                rate = self.base_rate_eps * 10  # 10x spike
                event_type = "spike"
            else:
                rate = normal_rate
                event_type = "normal"
            
            event = self._create_synthetic_event(f"{event_type}_event_{event_id}", current_time)
            events.append(event)
            
            current_time += 1.0 / rate
            event_id += 1
        
        return events
    
    def _periodic_pattern(self, duration: int) -> List[Event]:
        """Generate periodic load pattern."""
        events = []
        current_time = 0
        event_id = 0
        period = 10.0  # 10-second period
        
        while current_time < duration:
            # Sinusoidal rate variation
            phase = (current_time % period) / period * 2 * 3.14159
            rate_multiplier = 1 + 2 * (0.5 + 0.5 * random.random())  # 1x to 3x
            current_rate = self.base_rate_eps * rate_multiplier
            
            event = self._create_synthetic_event(f"periodic_event_{event_id}", current_time)
            events.append(event)
            
            current_time += 1.0 / current_rate
            event_id += 1
        
        return events
    
    def _create_synthetic_event(self, event_id: str, timestamp: float) -> Event:
        """Create a synthetic event for testing."""
        # Create a simple event with some random properties
        payload = {
            'id': event_id,
            'timestamp': timestamp,
            'value': random.uniform(0, 100),
            'category': random.choice(['A', 'B', 'C', 'D']),
            'priority': random.uniform(0, 1),
            'importance': random.uniform(0, 1)
        }
        
        # Create event (assuming Event constructor takes timestamp and payload)
        try:
            event = Event(timestamp, payload)
            event.event_type = payload['category']
            return event
        except Exception:
            # Fallback if Event constructor is different
            event = Event()
            event.timestamp = timestamp
            event.payload = payload
            event.event_type = payload['category']
            return event


class LoadSheddingBenchmark:
    """
    Comprehensive benchmarking suite for load shedding strategies.
    """
    
    def __init__(self, workload_generator: Optional[SyntheticWorkloadGenerator] = None):
        """Initialize the benchmark suite."""
        self.workload_generator = workload_generator or SyntheticWorkloadGenerator()
        self.results_history = []
    
    def run_synthetic_workload_test(self, 
                                  strategies: List[LoadSheddingStrategy], 
                                  patterns: List[Pattern],
                                  test_duration_seconds: int = 60) -> Dict[str, Any]:
        """
        Run comprehensive tests with synthetic workloads.
        
        Args:
            strategies: List of load shedding strategies to test
            patterns: List of patterns for testing
            test_duration_seconds: Duration of each test
            
        Returns:
            Dict containing test results
        """
        results = {}
        workload_patterns = ['constant_low', 'constant_high', 'bursty', 
                           'gradual_increase', 'spike_pattern', 'periodic']
        
        for strategy in strategies:
            strategy_name = strategy.name
            results[strategy_name] = {}
            
            for workload_pattern in workload_patterns:
                print(f"Testing {strategy_name} with {workload_pattern} workload...")
                
                # Generate synthetic workload
                events = self.workload_generator.generate_workload(
                    workload_pattern, test_duration_seconds
                )
                
                # Run test
                test_result = self._run_single_strategy_test(
                    strategy, patterns, events, f"{strategy_name}_{workload_pattern}"
                )
                
                results[strategy_name][workload_pattern] = test_result
        
        # Store results
        benchmark_result = {
            'timestamp': datetime.now().isoformat(),
            'test_type': 'synthetic_workload',
            'test_duration_seconds': test_duration_seconds,
            'strategies_tested': len(strategies),
            'workload_patterns_tested': len(workload_patterns),
            'results': results
        }
        
        self.results_history.append(benchmark_result)
        return benchmark_result
    
    def run_stress_test(self, 
                       strategy: LoadSheddingStrategy,
                       patterns: List[Pattern],
                       max_load_multiplier: float = 10.0,
                       step_duration_seconds: int = 30) -> Dict[str, Any]:
        """
        Run stress test with gradually increasing load.
        
        Args:
            strategy: Load shedding strategy to test
            patterns: Patterns for testing
            max_load_multiplier: Maximum load multiplier to test
            step_duration_seconds: Duration of each load step
            
        Returns:
            Dict containing stress test results
        """
        print(f"Running stress test for {strategy.name}...")
        
        results = {}
        load_multipliers = [1.0, 2.0, 3.0, 5.0, 7.0, 10.0]
        if max_load_multiplier not in load_multipliers:
            load_multipliers.append(max_load_multiplier)
        load_multipliers = [x for x in load_multipliers if x <= max_load_multiplier]
        load_multipliers.sort()
        
        for multiplier in load_multipliers:
            print(f"  Testing at {multiplier}x load...")
            
            # Generate high-load events
            base_generator = SyntheticWorkloadGenerator(
                base_rate_eps=self.workload_generator.base_rate_eps * multiplier
            )
            events = base_generator.generate_workload('constant_high', step_duration_seconds)
            
            # Run test
            test_result = self._run_single_strategy_test(
                strategy, patterns, events, f"stress_{multiplier}x"
            )
            
            results[f"{multiplier}x_load"] = test_result
            
            # Check if system became unresponsive
            if test_result['metrics']['avg_latency_ms'] > 10000:  # 10 second threshold
                print(f"  System became unresponsive at {multiplier}x load, stopping stress test")
                break
        
        return {
            'timestamp': datetime.now().isoformat(),
            'test_type': 'stress_test',
            'strategy': strategy.name,
            'max_load_tested': max(load_multipliers),
            'results': results
        }
    
    def run_comparison_benchmark(self, 
                               strategies: List[LoadSheddingStrategy],
                               patterns: List[Pattern],
                               test_scenarios: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run comparative benchmark across multiple strategies.
        
        Args:
            strategies: List of strategies to compare
            patterns: Patterns for testing
            test_scenarios: List of test scenarios to run
            
        Returns:
            Dict containing comparison results
        """
        if test_scenarios is None:
            test_scenarios = ['bursty', 'gradual_increase', 'spike_pattern']
        
        comparison_results = {}
        
        for scenario in test_scenarios:
            print(f"Running comparison for scenario: {scenario}")
            scenario_results = {}
            
            # Generate workload for this scenario
            events = self.workload_generator.generate_workload(scenario, 60)
            
            for strategy in strategies:
                print(f"  Testing {strategy.name}...")
                result = self._run_single_strategy_test(
                    strategy, patterns, events, f"comparison_{scenario}_{strategy.name}"
                )
                scenario_results[strategy.name] = result
            
            comparison_results[scenario] = scenario_results
        
        # Calculate comparative metrics
        comparative_analysis = self._analyze_comparative_results(comparison_results)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'test_type': 'comparison_benchmark',
            'scenarios_tested': test_scenarios,
            'strategies_compared': [s.name for s in strategies],
            'results': comparison_results,
            'analysis': comparative_analysis
        }
    
    def _run_single_strategy_test(self, 
                                strategy: LoadSheddingStrategy,
                                patterns: List[Pattern], 
                                events: List[Event],
                                test_name: str) -> Dict[str, Any]:
        """
        Run a single test with a specific strategy.
        
        Returns:
            Dict containing test results and metrics
        """
        # Initialize components
        load_monitor = LoadMonitor()
        metrics_collector = LoadSheddingMetrics()
        
        # Reset strategy statistics
        strategy.reset_statistics()
        
        # Start metrics collection
        metrics_collector.start_collection()
        start_time = time.time()
        
        processed_events = 0
        matched_patterns = 0
        
        try:
            # Simulate event processing
            for event in events:
                # Simulate system load
                current_load = self._simulate_system_load(processed_events, len(events))
                
                # Update load monitor
                load_monitor.update_event_metrics(
                    queue_size=max(0, len(events) - processed_events),
                    events_processed=processed_events,
                    latest_latency_ms=random.uniform(10, 200)  # Simulated latency
                )
                
                load_level = load_monitor.assess_load_level()
                
                # Apply load shedding decision
                should_drop = strategy.should_drop_event(event, load_level)
                
                if should_drop:
                    metrics_collector.record_event_dropped()
                else:
                    # Simulate event processing
                    processing_start = time.time()
                    time.sleep(random.uniform(0.001, 0.005))  # Simulate processing time
                    processing_time = (time.time() - processing_start) * 1000
                    
                    metrics_collector.record_event_processed(latency_ms=processing_time)
                    processed_events += 1
                    
                    # Simulate pattern matching (random chance)
                    if random.random() < 0.1:  # 10% chance of pattern match
                        matched_patterns += 1
                        metrics_collector.record_pattern_matched("test_pattern")
                
                # Record system snapshot periodically
                if processed_events % 100 == 0:
                    system_metrics = load_monitor.get_current_metrics()
                    metrics_collector.record_system_snapshot(
                        load_level=load_level.value,
                        cpu_percent=system_metrics.get('cpu_percent', 0.0),
                        memory_percent=system_metrics.get('memory_percent', 0.0),
                        queue_size=system_metrics.get('queue_size', 0)
                    )
        
        except Exception as e:
            print(f"Error during test {test_name}: {e}")
        
        finally:
            # Stop metrics collection
            metrics_collector.stop_collection()
            end_time = time.time()
        
        # Generate results
        test_duration = end_time - start_time
        summary = metrics_collector.get_summary_report()
        
        return {
            'test_name': test_name,
            'strategy_name': strategy.name,
            'test_duration_seconds': test_duration,
            'total_events': len(events),
            'events_processed': processed_events,
            'events_dropped': metrics_collector.events_dropped,
            'patterns_matched': matched_patterns,
            'drop_rate': metrics_collector.get_current_drop_rate(),
            'avg_throughput_eps': metrics_collector.get_average_throughput(),
            'avg_latency_ms': metrics_collector.get_average_latency(),
            'metrics': summary,
            'strategy_stats': strategy.get_drop_rate() if hasattr(strategy, 'get_drop_rate') else 0.0
        }
    
    def _simulate_system_load(self, processed_events: int, total_events: int) -> float:
        """Simulate varying system load during testing."""
        progress = processed_events / max(total_events, 1)
        
        # Add some random variation
        base_load = progress * 0.8 + random.uniform(0, 0.2)
        
        # Add periodic spikes
        spike_factor = 1.0 + 0.5 * abs(random.random() - 0.95) * 10 if random.random() > 0.95 else 1.0
        
        return min(1.0, base_load * spike_factor)
    
    def _analyze_comparative_results(self, results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze comparative results across strategies."""
        analysis = {
            'best_throughput': {},
            'best_latency': {},
            'best_quality': {},
            'most_stable': {},
            'overall_ranking': {}
        }
        
        for scenario, scenario_results in results.items():
            # Find best in each category
            best_throughput = max(scenario_results.items(), 
                                key=lambda x: x[1]['avg_throughput_eps'])
            best_latency = min(scenario_results.items(), 
                             key=lambda x: x[1]['avg_latency_ms'])
            best_quality = min(scenario_results.items(), 
                             key=lambda x: x[1]['drop_rate'])
            
            analysis['best_throughput'][scenario] = {
                'strategy': best_throughput[0],
                'value': best_throughput[1]['avg_throughput_eps']
            }
            analysis['best_latency'][scenario] = {
                'strategy': best_latency[0], 
                'value': best_latency[1]['avg_latency_ms']
            }
            analysis['best_quality'][scenario] = {
                'strategy': best_quality[0],
                'value': best_quality[1]['drop_rate']
            }
        
        return analysis
    
    def get_benchmark_history(self) -> List[Dict[str, Any]]:
        """Get history of all benchmark runs."""
        return self.results_history.copy()
    
    def clear_history(self):
        """Clear benchmark history."""
        self.results_history.clear()