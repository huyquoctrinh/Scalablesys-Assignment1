#!/usr/bin/env python3
"""
Load Shedding Demo and Benchmark Script for OpenCEP

This script demonstrates the load shedding capabilities of the enhanced OpenCEP system.
It includes examples of different load shedding strategies, synthetic workload generation,
and comprehensive performance evaluation.

Usage:
    python load_shedding_demo.py [options]
    
Options:
    --strategy STRATEGY    Load shedding strategy (probabilistic, semantic, adaptive, none)
    --duration SECONDS     Test duration in seconds (default: 60)
    --workload PATTERN     Workload pattern (constant_low, bursty, spike_pattern, etc.)
    --output-dir DIR       Output directory for reports and visualizations
    --comparison           Run strategy comparison benchmark
    --stress-test          Run stress testing
"""

import argparse
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from CEP import CEP
from base.Pattern import Pattern
from base.Event import Event
from stream.Stream import InputStream
from base.DataFormatter import DataFormatter

# Load shedding imports
try:
    from loadshedding import (
        LoadSheddingConfig, PresetConfigs,
        LoadSheddingBenchmark, SyntheticWorkloadGenerator,
        PerformanceEvaluator, LoadSheddingReporter,
        ProbabilisticLoadShedding, SemanticLoadShedding, 
        AdaptiveLoadShedding, NoLoadShedding
    )
    LOAD_SHEDDING_AVAILABLE = True
except ImportError as e:
    print(f"Load shedding not available: {e}")
    LOAD_SHEDDING_AVAILABLE = False
    sys.exit(1)


class SimpleInputStream(InputStream):
    """Simple input stream implementation for demonstration."""
    
    def __init__(self, events):
        self.events = events
        self.index = 0
    
    def get_item(self):
        if self.index >= len(self.events):
            raise StopIteration
        
        event = self.events[self.index]
        self.index += 1
        return event
    
    def has_next(self):
        return self.index < len(self.events)


class SimpleOutputStream:
    """Simple output stream for collecting matches."""
    
    def __init__(self):
        self.matches = []
    
    def add_item(self, match):
        self.matches.append(match)
    
    def get_matches(self):
        return self.matches.copy()


def create_sample_patterns():
    """Create sample patterns for demonstration."""
    # For this demo, we'll create simple mock patterns
    # In a real scenario, you would use actual Pattern objects
    patterns = []
    
    for i in range(5):
        pattern = Pattern()  # Assuming Pattern() constructor exists
        pattern.name = f"SamplePattern_{i}"
        patterns.append(pattern)
    
    return patterns


def run_basic_demo():
    """Run a basic load shedding demonstration."""
    print("=" * 60)
    print("BASIC LOAD SHEDDING DEMONSTRATION")
    print("=" * 60)
    
    # Create sample patterns
    patterns = create_sample_patterns()
    print(f"Created {len(patterns)} sample patterns")
    
    # Generate synthetic workload
    workload_generator = SyntheticWorkloadGenerator(base_rate_eps=1000)
    events = workload_generator.generate_workload('bursty', 30)
    print(f"Generated {len(events)} events with bursty pattern")
    
    # Test different load shedding configurations
    configs = {
        'No Load Shedding': LoadSheddingConfig(enabled=False),
        'Conservative': PresetConfigs.conservative(),
        'Aggressive': PresetConfigs.aggressive(),
        'Quality Preserving': PresetConfigs.quality_preserving(),
        'Adaptive Learning': PresetConfigs.adaptive_learning()
    }
    
    results = {}
    
    for config_name, config in configs.items():
        print(f"\nTesting configuration: {config_name}")
        print("-" * 40)
        
        # Create CEP instance with load shedding
        cep = CEP(patterns, load_shedding_config=config)
        
        # Create input/output streams
        input_stream = SimpleInputStream(events)
        output_stream = SimpleOutputStream()
        data_formatter = DataFormatter()
        
        # Run CEP processing
        start_time = datetime.now()
        try:
            duration = cep.run(input_stream, output_stream, data_formatter)
            print(f"Processing completed in {duration:.2f} seconds")
            
            # Get load shedding statistics
            if cep.is_load_shedding_enabled():
                stats = cep.get_load_shedding_statistics()
                print(f"Strategy: {stats['strategy']}")
                print(f"Events processed: {stats['events_processed']}")
                print(f"Events dropped: {stats['events_dropped']}")
                print(f"Drop rate: {stats['drop_rate']:.1%}")
                print(f"Average throughput: {stats['avg_throughput_eps']:.2f} EPS")
                
                # Store results for comparison
                results[config_name] = {
                    'duration': duration,
                    'stats': stats,
                    'metrics': cep.get_load_shedding_metrics()
                }
            else:
                print("Load shedding disabled")
                results[config_name] = {
                    'duration': duration,
                    'stats': None,
                    'metrics': None
                }
                
        except Exception as e:
            print(f"Error during processing: {e}")
            results[config_name] = {'error': str(e)}
    
    return results


def run_comprehensive_benchmark(args):
    """Run comprehensive benchmarking across multiple strategies."""
    print("=" * 60)
    print("COMPREHENSIVE LOAD SHEDDING BENCHMARK")
    print("=" * 60)
    
    # Create patterns
    patterns = create_sample_patterns()
    
    # Create strategies to test
    strategies = [
        NoLoadShedding(),
        ProbabilisticLoadShedding(),
        SemanticLoadShedding(pattern_priorities={'SamplePattern_0': 8.0, 'SamplePattern_1': 6.0}),
        AdaptiveLoadShedding()
    ]
    
    # Initialize benchmark suite
    benchmark = LoadSheddingBenchmark()
    evaluator = PerformanceEvaluator()
    reporter = LoadSheddingReporter(args.output_dir)
    
    print(f"Testing {len(strategies)} strategies for {args.duration} seconds")
    
    # Run synthetic workload tests
    print("\nRunning synthetic workload tests...")
    benchmark_results = benchmark.run_synthetic_workload_test(
        strategies, patterns, args.duration
    )
    
    # Run performance evaluation
    print("Evaluating performance...")
    evaluation_results = []
    
    for strategy_name, strategy_results in benchmark_results.get('results', {}).items():
        for workload, result in strategy_results.items():
            # Create mock metrics for evaluation
            # In real implementation, you'd use actual LoadSheddingMetrics
            mock_metrics = type('MockMetrics', (), {
                'get_duration_seconds': lambda: args.duration,
                'get_current_drop_rate': lambda: result.get('drop_rate', 0.0),
                'get_average_throughput': lambda: result.get('avg_throughput_eps', 0.0),
                'get_average_latency': lambda: result.get('avg_latency_ms', 0.0),
                'events_processed': result.get('events_processed', 0),
                'events_dropped': result.get('events_dropped', 0),
                'patterns_matched': result.get('patterns_matched', 0),
                'processing_latencies': [result.get('avg_latency_ms', 0.0)] * 10,
                'end_to_end_latencies': [],
                'snapshots': [],
                'false_negatives': 0,
                'false_positives': 0,
                'pattern_completeness_ratios': {},
                'load_shedding_activations': 0,
                'recovery_times': [],
                'strategy_switches': 0
            })()
            
            evaluation = evaluator.evaluate_single_test(
                mock_metrics,
                ground_truth_matches=int(result.get('events_processed', 0) * 0.1)
            )
            evaluation['strategy_name'] = strategy_name
            evaluation['workload'] = workload
            evaluation_results.append(evaluation)
    
    # Compare strategies
    print("Comparing strategies...")
    comparison = evaluator.compare_strategies(evaluation_results)
    
    # Generate reports
    print(f"Generating reports in {args.output_dir}...")
    
    # Comprehensive report
    report_path = reporter.generate_comprehensive_report(
        benchmark_results, evaluation_results, "benchmark_report"
    )
    print(f"Comprehensive report: {report_path}")
    
    # Strategy comparison chart
    if comparison:
        chart_path = reporter.create_strategy_comparison_chart(comparison)
        if chart_path:
            print(f"Strategy comparison chart: {chart_path}")
    
    # Print summary to console
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)
    
    if 'best_in_category' in comparison:
        for category, info in comparison['best_in_category'].items():
            print(f"Best {info['display_name']}: {info['strategy']} ({info['value']:.3f})")
    
    return benchmark_results, evaluation_results, comparison


def run_stress_test(args):
    """Run stress testing with increasing load."""
    print("=" * 60)
    print("STRESS TEST")
    print("=" * 60)
    
    patterns = create_sample_patterns()
    strategy = ProbabilisticLoadShedding()  # Use probabilistic for stress test
    
    benchmark = LoadSheddingBenchmark()
    
    print("Running stress test with increasing load multipliers...")
    stress_results = benchmark.run_stress_test(
        strategy, patterns, max_load_multiplier=10.0, step_duration_seconds=30
    )
    
    print("\nStress Test Results:")
    print("-" * 30)
    
    for load_level, result in stress_results.get('results', {}).items():
        print(f"{load_level}:")
        print(f"  Events processed: {result.get('events_processed', 0)}")
        print(f"  Drop rate: {result.get('drop_rate', 0.0):.1%}")
        print(f"  Avg latency: {result.get('avg_latency_ms', 0.0):.2f} ms")
        print(f"  Throughput: {result.get('avg_throughput_eps', 0.0):.2f} EPS")
    
    return stress_results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Load Shedding Demo and Benchmark')
    parser.add_argument('--strategy', choices=['probabilistic', 'semantic', 'adaptive', 'none'],
                       default='probabilistic', help='Load shedding strategy')
    parser.add_argument('--duration', type=int, default=60,
                       help='Test duration in seconds')
    parser.add_argument('--workload', default='bursty',
                       help='Workload pattern')
    parser.add_argument('--output-dir', default='load_shedding_output',
                       help='Output directory for reports')
    parser.add_argument('--comparison', action='store_true',
                       help='Run strategy comparison benchmark')
    parser.add_argument('--stress-test', action='store_true',
                       help='Run stress testing')
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("OpenCEP Load Shedding Demo")
    print(f"Load shedding available: {LOAD_SHEDDING_AVAILABLE}")
    print(f"Output directory: {args.output_dir}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not LOAD_SHEDDING_AVAILABLE:
        print("Load shedding module not available. Exiting.")
        return
    
    try:
        if args.comparison:
            # Run comprehensive benchmark
            benchmark_results, evaluation_results, comparison = run_comprehensive_benchmark(args)
        elif args.stress_test:
            # Run stress test
            stress_results = run_stress_test(args)
        else:
            # Run basic demo
            demo_results = run_basic_demo()
        
        print("\nDemo completed successfully!")
        print(f"Results and reports available in: {args.output_dir}")
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Error during demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()