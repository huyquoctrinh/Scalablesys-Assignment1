"""
Comprehensive Benchmarking Suite for Hot Path Load Shedding
This module provides detailed performance evaluation and recall analysis.
"""

import time
import os
import json
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
import statistics
import matplotlib.pyplot as plt
import pandas as pd
from collections import defaultdict

from .LoadSheddingConfig import LoadSheddingConfig, PresetConfigs
from .HotPathLoadSheddingStrategy import HotPathLoadSheddingStrategy, LatencyAwareHotPathLoadShedding
from .LoadMonitor import LoadLevel
from base.Event import Event
from base.Pattern import Pattern
from base.PatternMatch import PatternMatch


class HotPathBenchmark:
    """
    Comprehensive benchmarking suite for hot path load shedding evaluation.
    
    This class provides detailed performance analysis including:
    - Recall evaluation under different latency bounds
    - Throughput analysis
    - Load shedding effectiveness
    - Pattern completion rates
    """
    
    def __init__(self, output_dir: str = "hotpath_benchmark_results"):
        """
        Initialize the hot path benchmark suite.
        
        Args:
            output_dir: Directory to save benchmark results
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Benchmark configurations
        self.latency_bounds = [0.1, 0.3, 0.5, 0.7, 0.9]  # 10%, 30%, 50%, 70%, 90%
        self.load_levels = [LoadLevel.NORMAL, LoadLevel.MEDIUM, LoadLevel.HIGH, LoadLevel.CRITICAL]
        
        # Results storage
        self.benchmark_results = {}
        self.recall_results = {}
        self.latency_results = {}
        self.throughput_results = {}
        
        # Statistics tracking
        self.total_benchmarks = 0
        self.completed_benchmarks = 0
    
    def run_comprehensive_benchmark(self, 
                                  patterns: List[Pattern],
                                  input_stream,
                                  data_formatter,
                                  max_events: int = 10000,
                                  iterations: int = 3) -> Dict[str, Any]:
        """
        Run comprehensive benchmark across all configurations.
        
        Args:
            patterns: List of patterns to test
            input_stream: Input stream for testing
            data_formatter: Data formatter for events
            max_events: Maximum number of events to process
            iterations: Number of iterations for statistical significance
            
        Returns:
            Dictionary containing all benchmark results
        """
        print("=" * 80)
        print("HOT PATH LOAD SHEDDING COMPREHENSIVE BENCHMARK")
        print("=" * 80)
        print(f"Patterns: {len(patterns)}")
        print(f"Max events: {max_events:,}")
        print(f"Iterations: {iterations}")
        print(f"Output directory: {self.output_dir}")
        print()
        
        # Test configurations
        configs = self._create_test_configurations()
        
        all_results = {}
        
        for config_name, config in configs.items():
            print(f"Testing configuration: {config_name}")
            print("-" * 50)
            
            config_results = self._run_configuration_benchmark(
                patterns, input_stream, data_formatter, 
                config, max_events, iterations
            )
            
            all_results[config_name] = config_results
            self.benchmark_results[config_name] = config_results
            
            print(f"✓ Completed {config_name}")
            print()
        
        # Generate comprehensive analysis
        self._generate_comprehensive_analysis(all_results)
        
        # Save results
        self._save_benchmark_results(all_results)
        
        print("=" * 80)
        print("BENCHMARK COMPLETED SUCCESSFULLY")
        print("=" * 80)
        
        return all_results
    
    def _create_test_configurations(self) -> Dict[str, LoadSheddingConfig]:
        """Create test configurations for benchmarking."""
        configs = {}
        
        # Baseline (no load shedding)
        configs['No Load Shedding'] = LoadSheddingConfig(enabled=False)
        
        # Conservative load shedding
        configs['Conservative'] = PresetConfigs.conservative()
        
        # Hot path specific configurations
        configs['Hot Path Semantic'] = LoadSheddingConfig(
            strategy_name='semantic',
            pattern_priorities={
                'RushHourHotPath': 10.0,
                'SubscriberHotPath': 8.0,
                'PopularStationHotPath': 7.0,
                'LongDistanceHotPath': 6.0,
                'RegularHotPath': 5.0
            },
            importance_attributes=['importance', 'priority', 'time_criticality', 'station_importance'],
            memory_threshold=0.75,
            cpu_threshold=0.85
        )
        
        # Latency-aware configurations for each bound
        for bound in self.latency_bounds:
            configs[f'Latency {int(bound*100)}%'] = LoadSheddingConfig(
                strategy_name='adaptive',
                memory_threshold=0.7,
                cpu_threshold=0.8,
                latency_threshold_ms=100 * bound
            )
        
        # Aggressive load shedding
        configs['Aggressive'] = PresetConfigs.aggressive()
        
        return configs
    
    def _run_configuration_benchmark(self, 
                                   patterns: List[Pattern],
                                   input_stream,
                                   data_formatter,
                                   config: LoadSheddingConfig,
                                   max_events: int,
                                   iterations: int) -> Dict[str, Any]:
        """Run benchmark for a specific configuration."""
        iteration_results = []
        
        for iteration in range(iterations):
            print(f"  Iteration {iteration + 1}/{iterations}")
            
            # Import CEP here to avoid circular import
            from CEP import CEP
            
            # Create CEP instance
            if config.enabled:
                cep = CEP(patterns, load_shedding_config=config)
            else:
                cep = CEP(patterns)
            
            # Create output stream
            output_stream = self._create_output_stream()
            
            # Run processing
            start_time = time.time()
            duration = cep.run(input_stream, output_stream, data_formatter)
            end_time = time.time()
            
            # Collect results
            iteration_result = {
                'iteration': iteration + 1,
                'duration_seconds': duration,
                'wall_clock_seconds': end_time - start_time,
                'events_processed': getattr(input_stream, 'count', 0),
                'matches_found': len(output_stream.get_matches()) if hasattr(output_stream, 'get_matches') else 0,
                'load_shedding_stats': cep.get_load_shedding_statistics() if cep.is_load_shedding_enabled() else None,
                'throughput_eps': 0,
                'avg_latency_ms': 0
            }
            
            # Calculate throughput
            if duration > 0:
                iteration_result['throughput_eps'] = iteration_result['events_processed'] / duration
            
            # Calculate average latency
            if cep.is_load_shedding_enabled():
                ls_stats = cep.get_load_shedding_statistics()
                if ls_stats:
                    iteration_result['avg_latency_ms'] = ls_stats.get('avg_latency_ms', 0)
            
            iteration_results.append(iteration_result)
        
        # Calculate statistics across iterations
        return self._calculate_configuration_statistics(iteration_results)
    
    def _create_output_stream(self):
        """Create output stream for collecting results."""
        class BenchmarkOutputStream:
            def __init__(self):
                self.matches = []
            
            def add_item(self, item):
                self.matches.append(item)
            
            def get_matches(self):
                return self.matches
            
            def close(self):
                pass
        
        return BenchmarkOutputStream()
    
    def _calculate_configuration_statistics(self, iteration_results: List[Dict]) -> Dict[str, Any]:
        """Calculate statistics across iterations."""
        if not iteration_results:
            return {}
        
        # Extract metrics
        durations = [r['duration_seconds'] for r in iteration_results]
        wall_clocks = [r['wall_clock_seconds'] for r in iteration_results]
        events_processed = [r['events_processed'] for r in iteration_results]
        matches_found = [r['matches_found'] for r in iteration_results]
        throughputs = [r['throughput_eps'] for r in iteration_results]
        latencies = [r['avg_latency_ms'] for r in iteration_results]
        
        # Calculate statistics
        stats = {
            'iterations': len(iteration_results),
            'duration': {
                'mean': statistics.mean(durations),
                'std': statistics.stdev(durations) if len(durations) > 1 else 0,
                'min': min(durations),
                'max': max(durations)
            },
            'wall_clock': {
                'mean': statistics.mean(wall_clocks),
                'std': statistics.stdev(wall_clocks) if len(wall_clocks) > 1 else 0,
                'min': min(wall_clocks),
                'max': max(wall_clocks)
            },
            'events_processed': {
                'mean': statistics.mean(events_processed),
                'std': statistics.stdev(events_processed) if len(events_processed) > 1 else 0,
                'min': min(events_processed),
                'max': max(events_processed)
            },
            'matches_found': {
                'mean': statistics.mean(matches_found),
                'std': statistics.stdev(matches_found) if len(matches_found) > 1 else 0,
                'min': min(matches_found),
                'max': max(matches_found)
            },
            'throughput_eps': {
                'mean': statistics.mean(throughputs),
                'std': statistics.stdev(throughputs) if len(throughputs) > 1 else 0,
                'min': min(throughputs),
                'max': max(throughputs)
            },
            'avg_latency_ms': {
                'mean': statistics.mean(latencies),
                'std': statistics.stdev(latencies) if len(latencies) > 1 else 0,
                'min': min(latencies),
                'max': max(latencies)
            }
        }
        
        # Add load shedding statistics if available
        ls_stats_list = [r['load_shedding_stats'] for r in iteration_results if r['load_shedding_stats']]
        if ls_stats_list:
            drop_rates = [s['drop_rate'] for s in ls_stats_list]
            events_dropped = [s['events_dropped'] for s in ls_stats_list]
            
            stats['load_shedding'] = {
                'drop_rate': {
                    'mean': statistics.mean(drop_rates),
                    'std': statistics.stdev(drop_rates) if len(drop_rates) > 1 else 0,
                    'min': min(drop_rates),
                    'max': max(drop_rates)
                },
                'events_dropped': {
                    'mean': statistics.mean(events_dropped),
                    'std': statistics.stdev(events_dropped) if len(events_dropped) > 1 else 0,
                    'min': min(events_dropped),
                    'max': max(events_dropped)
                }
            }
        
        return stats
    
    def evaluate_recall_latency(self, 
                               patterns: List[Pattern],
                               input_stream,
                               data_formatter,
                               max_events: int = 10000) -> Dict[str, Any]:
        """
        Evaluate recall under different latency bounds.
        
        This is a key requirement: measure recall under latency bounds set to
        10%, 30%, 50%, 70%, 90% of the original latency without load shedding.
        """
        print("=" * 80)
        print("RECALL-LATENCY EVALUATION")
        print("=" * 80)
        
        # First, get baseline (no load shedding) results
        print("Running baseline (no load shedding)...")
        baseline_config = LoadSheddingConfig(enabled=False)
        baseline_results = self._run_configuration_benchmark(
            patterns, input_stream, data_formatter, 
            baseline_config, max_events, 1
        )
        
        baseline_matches = baseline_results['matches_found']['mean']
        baseline_latency = baseline_results['avg_latency_ms']['mean']
        
        print(f"Baseline matches: {baseline_matches:.0f}")
        print(f"Baseline latency: {baseline_latency:.2f} ms")
        print()
        
        # Test each latency bound
        recall_results = {}
        
        for bound in self.latency_bounds:
            target_latency = baseline_latency * bound
            print(f"Testing latency bound: {int(bound*100)}% ({target_latency:.2f} ms)")
            
            # Create latency-aware configuration
            config = LoadSheddingConfig(
                strategy_name='adaptive',
                memory_threshold=0.7,
                cpu_threshold=0.8,
                latency_threshold_ms=target_latency
            )
            
            # Run test
            test_results = self._run_configuration_benchmark(
                patterns, input_stream, data_formatter,
                config, max_events, 1
            )
            
            # Calculate recall
            test_matches = test_results['matches_found']['mean']
            recall = test_matches / max(baseline_matches, 1)
            achieved_latency = test_results['avg_latency_ms']['mean']
            
            recall_results[f'{int(bound*100)}%'] = {
                'target_latency_ms': target_latency,
                'achieved_latency_ms': achieved_latency,
                'recall': recall,
                'matches_found': test_matches,
                'baseline_matches': baseline_matches,
                'latency_ratio': achieved_latency / max(baseline_latency, 0.001)
            }
            
            print(f"  Recall: {recall:.3f} ({test_matches:.0f}/{baseline_matches:.0f})")
            print(f"  Achieved latency: {achieved_latency:.2f} ms")
            print()
        
        self.recall_results = recall_results
        return recall_results
    
    def _generate_comprehensive_analysis(self, all_results: Dict[str, Any]):
        """Generate comprehensive analysis of benchmark results."""
        print("=" * 80)
        print("COMPREHENSIVE ANALYSIS")
        print("=" * 80)
        
        # Performance comparison
        self._analyze_performance_comparison(all_results)
        
        # Load shedding effectiveness
        self._analyze_load_shedding_effectiveness(all_results)
        
        # Recall-latency trade-offs
        if self.recall_results:
            self._analyze_recall_latency_tradeoffs()
        
        # Generate visualizations
        self._generate_visualizations(all_results)
    
    def _analyze_performance_comparison(self, all_results: Dict[str, Any]):
        """Analyze performance across different configurations."""
        print("\nPERFORMANCE COMPARISON:")
        print("-" * 40)
        
        # Find baseline
        baseline = all_results.get('No Load Shedding')
        if not baseline:
            print("No baseline found for comparison")
            return
        
        baseline_throughput = baseline['throughput_eps']['mean']
        baseline_latency = baseline['avg_latency_ms']['mean']
        baseline_matches = baseline['matches_found']['mean']
        
        print(f"Baseline (No Load Shedding):")
        print(f"  Throughput: {baseline_throughput:.1f} EPS")
        print(f"  Latency: {baseline_latency:.2f} ms")
        print(f"  Matches: {baseline_matches:.0f}")
        print()
        
        # Compare other configurations
        for config_name, results in all_results.items():
            if config_name == 'No Load Shedding':
                continue
            
            throughput = results['throughput_eps']['mean']
            latency = results['avg_latency_ms']['mean']
            matches = results['matches_found']['mean']
            
            throughput_ratio = throughput / max(baseline_throughput, 0.001)
            latency_ratio = latency / max(baseline_latency, 0.001)
            recall = matches / max(baseline_matches, 1)
            
            print(f"{config_name}:")
            print(f"  Throughput: {throughput:.1f} EPS ({throughput_ratio:.2f}x)")
            print(f"  Latency: {latency:.2f} ms ({latency_ratio:.2f}x)")
            print(f"  Recall: {recall:.3f} ({matches:.0f} matches)")
            
            if 'load_shedding' in results:
                drop_rate = results['load_shedding']['drop_rate']['mean']
                print(f"  Drop Rate: {drop_rate:.1%}")
            
            print()
    
    def _analyze_load_shedding_effectiveness(self, all_results: Dict[str, Any]):
        """Analyze load shedding effectiveness."""
        print("\nLOAD SHEDDING EFFECTIVENESS:")
        print("-" * 40)
        
        # Find configurations with load shedding
        ls_configs = {k: v for k, v in all_results.items() 
                     if 'load_shedding' in v and v['load_shedding']}
        
        if not ls_configs:
            print("No load shedding configurations found")
            return
        
        for config_name, results in ls_configs.items():
            drop_rate = results['load_shedding']['drop_rate']['mean']
            throughput = results['throughput_eps']['mean']
            latency = results['avg_latency_ms']['mean']
            
            # Calculate efficiency score (recall / (1 - drop_rate))
            recall = results['matches_found']['mean']
            baseline_matches = all_results.get('No Load Shedding', {}).get('matches_found', {}).get('mean', 1)
            recall_ratio = recall / max(baseline_matches, 1)
            
            efficiency = recall_ratio / max(1 - drop_rate, 0.001)
            
            print(f"{config_name}:")
            print(f"  Drop Rate: {drop_rate:.1%}")
            print(f"  Recall: {recall_ratio:.3f}")
            print(f"  Efficiency: {efficiency:.2f}")
            print(f"  Throughput: {throughput:.1f} EPS")
            print(f"  Latency: {latency:.2f} ms")
            print()
    
    def _analyze_recall_latency_tradeoffs(self):
        """Analyze recall-latency trade-offs."""
        print("\nRECALL-LATENCY TRADEOFFS:")
        print("-" * 40)
        
        for bound_name, results in self.recall_results.items():
            recall = results['recall']
            latency_ratio = results['latency_ratio']
            target_latency = results['target_latency_ms']
            achieved_latency = results['achieved_latency_ms']
            
            print(f"{bound_name} Latency Bound:")
            print(f"  Target: {target_latency:.2f} ms")
            print(f"  Achieved: {achieved_latency:.2f} ms")
            print(f"  Recall: {recall:.3f}")
            print(f"  Latency Ratio: {latency_ratio:.2f}")
            print()
    
    def _generate_visualizations(self, all_results: Dict[str, Any]):
        """Generate visualization plots."""
        print("\nGenerating visualizations...")
        
        # Throughput comparison
        self._plot_throughput_comparison(all_results)
        
        # Latency comparison
        self._plot_latency_comparison(all_results)
        
        # Recall-latency trade-offs
        if self.recall_results:
            self._plot_recall_latency_tradeoffs()
        
        print("Visualizations saved to", self.output_dir)
    
    def _plot_throughput_comparison(self, all_results: Dict[str, Any]):
        """Plot throughput comparison across configurations."""
        configs = list(all_results.keys())
        throughputs = [all_results[config]['throughput_eps']['mean'] for config in configs]
        
        plt.figure(figsize=(12, 6))
        bars = plt.bar(configs, throughputs, alpha=0.7, color='skyblue', edgecolor='navy')
        plt.title('Throughput Comparison Across Configurations')
        plt.xlabel('Configuration')
        plt.ylabel('Throughput (Events per Second)')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, throughput in zip(bars, throughputs):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{throughput:.1f}', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'throughput_comparison.png'), dpi=300, bbox_inches='tight')
        plt.close()
    
    def _plot_latency_comparison(self, all_results: Dict[str, Any]):
        """Plot latency comparison across configurations."""
        configs = list(all_results.keys())
        latencies = [all_results[config]['avg_latency_ms']['mean'] for config in configs]
        
        plt.figure(figsize=(12, 6))
        bars = plt.bar(configs, latencies, alpha=0.7, color='lightcoral', edgecolor='darkred')
        plt.title('Latency Comparison Across Configurations')
        plt.xlabel('Configuration')
        plt.ylabel('Average Latency (ms)')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, latency in zip(bars, latencies):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{latency:.1f}', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'latency_comparison.png'), dpi=300, bbox_inches='tight')
        plt.close()
    
    def _plot_recall_latency_tradeoffs(self):
        """Plot recall-latency trade-offs."""
        bounds = list(self.recall_results.keys())
        recalls = [self.recall_results[bound]['recall'] for bound in bounds]
        latency_ratios = [self.recall_results[bound]['latency_ratio'] for bound in bounds]
        
        plt.figure(figsize=(10, 6))
        plt.scatter(latency_ratios, recalls, s=100, alpha=0.7, color='green', edgecolor='darkgreen')
        
        # Add labels for each point
        for i, bound in enumerate(bounds):
            plt.annotate(bound, (latency_ratios[i], recalls[i]), 
                        xytext=(5, 5), textcoords='offset points')
        
        plt.xlabel('Latency Ratio (Achieved / Baseline)')
        plt.ylabel('Recall (Matches / Baseline Matches)')
        plt.title('Recall-Latency Trade-offs')
        plt.grid(True, alpha=0.3)
        
        # Add diagonal line for reference
        plt.plot([0, 1], [0, 1], 'r--', alpha=0.5, label='Perfect Trade-off')
        plt.legend()
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'recall_latency_tradeoffs.png'), dpi=300, bbox_inches='tight')
        plt.close()
    
    def _save_benchmark_results(self, all_results: Dict[str, Any]):
        """Save benchmark results to files."""
        # Save JSON results
        results_file = os.path.join(self.output_dir, 'benchmark_results.json')
        with open(results_file, 'w') as f:
            # Convert numpy types to native Python types for JSON serialization
            json_results = self._convert_to_json_serializable(all_results)
            json.dump(json_results, f, indent=2, default=str)
        
        # Save CSV summary
        self._save_csv_summary(all_results)
        
        # Save detailed report
        self._save_detailed_report(all_results)
    
    def _convert_to_json_serializable(self, obj):
        """Convert objects to JSON serializable format."""
        if isinstance(obj, dict):
            return {k: self._convert_to_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_json_serializable(item) for item in obj]
        elif hasattr(obj, 'value'):  # Enum
            return obj.value
        else:
            return obj
    
    def _save_csv_summary(self, all_results: Dict[str, Any]):
        """Save CSV summary of results."""
        summary_data = []
        
        for config_name, results in all_results.items():
            row = {
                'Configuration': config_name,
                'Throughput_EPS': results['throughput_eps']['mean'],
                'Latency_ms': results['avg_latency_ms']['mean'],
                'Matches_Found': results['matches_found']['mean'],
                'Duration_s': results['duration']['mean']
            }
            
            if 'load_shedding' in results:
                row['Drop_Rate'] = results['load_shedding']['drop_rate']['mean']
                row['Events_Dropped'] = results['load_shedding']['events_dropped']['mean']
            else:
                row['Drop_Rate'] = 0.0
                row['Events_Dropped'] = 0
            
            summary_data.append(row)
        
        df = pd.DataFrame(summary_data)
        csv_file = os.path.join(self.output_dir, 'benchmark_summary.csv')
        df.to_csv(csv_file, index=False)
    
    def _save_detailed_report(self, all_results: Dict[str, Any]):
        """Save detailed text report."""
        report_file = os.path.join(self.output_dir, 'detailed_report.txt')
        
        with open(report_file, 'w') as f:
            f.write("HOT PATH LOAD SHEDDING BENCHMARK REPORT\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total configurations tested: {len(all_results)}\n\n")
            
            # Write detailed results for each configuration
            for config_name, results in all_results.items():
                f.write(f"CONFIGURATION: {config_name}\n")
                f.write("-" * 30 + "\n")
                
                f.write(f"Throughput: {results['throughput_eps']['mean']:.2f} ± {results['throughput_eps']['std']:.2f} EPS\n")
                f.write(f"Latency: {results['avg_latency_ms']['mean']:.2f} ± {results['avg_latency_ms']['std']:.2f} ms\n")
                f.write(f"Matches: {results['matches_found']['mean']:.0f} ± {results['matches_found']['std']:.0f}\n")
                f.write(f"Duration: {results['duration']['mean']:.2f} ± {results['duration']['std']:.2f} s\n")
                
                if 'load_shedding' in results:
                    f.write(f"Drop Rate: {results['load_shedding']['drop_rate']['mean']:.1%}\n")
                    f.write(f"Events Dropped: {results['load_shedding']['events_dropped']['mean']:.0f}\n")
                
                f.write("\n")
            
            # Write recall-latency results if available
            if self.recall_results:
                f.write("RECALL-LATENCY EVALUATION\n")
                f.write("-" * 30 + "\n")
                
                for bound_name, results in self.recall_results.items():
                    f.write(f"{bound_name} Latency Bound:\n")
                    f.write(f"  Target Latency: {results['target_latency_ms']:.2f} ms\n")
                    f.write(f"  Achieved Latency: {results['achieved_latency_ms']:.2f} ms\n")
                    f.write(f"  Recall: {results['recall']:.3f}\n")
                    f.write(f"  Latency Ratio: {results['latency_ratio']:.2f}\n\n")
