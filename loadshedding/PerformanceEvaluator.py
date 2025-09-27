"""
Performance evaluation utilities for load shedding analysis.
This module provides tools to analyze and compare load shedding performance.
"""

import statistics
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import json

from .LoadSheddingMetrics import LoadSheddingMetrics


class PerformanceEvaluator:
    """
    Evaluates load shedding performance across multiple dimensions.
    
    This class provides comprehensive analysis of load shedding effectiveness,
    including throughput, latency, quality preservation, and resource utilization.
    """
    
    def __init__(self):
        """Initialize the performance evaluator."""
        self.evaluation_history = []
    
    def evaluate_single_test(self, 
                           metrics: LoadSheddingMetrics, 
                           ground_truth_matches: Optional[int] = None,
                           baseline_latency_ms: Optional[float] = None) -> Dict[str, Any]:
        """
        Evaluate performance of a single load shedding test.
        
        Args:
            metrics: Collected metrics from the test
            ground_truth_matches: Expected number of matches without load shedding
            baseline_latency_ms: Baseline latency without load shedding
            
        Returns:
            Dict containing comprehensive evaluation results
        """
        summary = metrics.get_summary_report()
        
        # Calculate core performance metrics
        throughput_metrics = self.calculate_throughput_metrics(metrics)
        latency_metrics = self.calculate_latency_metrics(metrics, baseline_latency_ms)
        quality_metrics = self.calculate_quality_metrics(metrics, ground_truth_matches)
        efficiency_metrics = self.calculate_efficiency_metrics(metrics)
        stability_metrics = self.calculate_stability_metrics(metrics)
        
        evaluation = {
            'evaluation_timestamp': datetime.now().isoformat(),
            'test_duration_seconds': metrics.get_duration_seconds(),
            'throughput': throughput_metrics,
            'latency': latency_metrics,
            'quality': quality_metrics,
            'efficiency': efficiency_metrics,
            'stability': stability_metrics,
            'overall_score': self._calculate_overall_score(
                throughput_metrics, latency_metrics, quality_metrics, efficiency_metrics
            ),
            'raw_summary': summary
        }
        
        self.evaluation_history.append(evaluation)
        return evaluation
    
    def calculate_throughput_metrics(self, metrics: LoadSheddingMetrics) -> Dict[str, float]:
        """Calculate throughput-related metrics."""
        duration = metrics.get_duration_seconds()
        
        if duration <= 0:
            return {
                'events_per_second': 0.0,
                'patterns_per_second': 0.0,
                'drop_rate': metrics.get_current_drop_rate(),
                'effective_throughput': 0.0,
                'processing_efficiency': 0.0
            }
        
        events_per_second = metrics.events_processed / duration
        patterns_per_second = metrics.patterns_matched / duration
        total_events = metrics.events_processed + metrics.events_dropped
        effective_throughput = metrics.patterns_matched / max(total_events, 1)
        processing_efficiency = metrics.events_processed / max(total_events, 1)
        
        return {
            'events_per_second': events_per_second,
            'patterns_per_second': patterns_per_second,
            'drop_rate': metrics.get_current_drop_rate(),
            'effective_throughput': effective_throughput,
            'processing_efficiency': processing_efficiency
        }
    
    def calculate_latency_metrics(self, 
                                metrics: LoadSheddingMetrics, 
                                baseline_latency_ms: Optional[float] = None) -> Dict[str, float]:
        """Calculate latency-related metrics."""
        if not metrics.processing_latencies:
            return {
                'avg_latency_ms': 0.0,
                'median_latency_ms': 0.0,
                'p95_latency_ms': 0.0,
                'p99_latency_ms': 0.0,
                'max_latency_ms': 0.0,
                'latency_improvement_ratio': 0.0,
                'latency_stability': 0.0
            }
        
        latencies = metrics.processing_latencies
        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        
        # Calculate percentiles
        sorted_latencies = sorted(latencies)
        n = len(sorted_latencies)
        p95_latency = sorted_latencies[min(int(0.95 * n), n-1)]
        p99_latency = sorted_latencies[min(int(0.99 * n), n-1)]
        max_latency = max(latencies)
        
        # Calculate latency improvement ratio
        latency_improvement_ratio = 0.0
        if baseline_latency_ms and baseline_latency_ms > 0:
            latency_improvement_ratio = (baseline_latency_ms - avg_latency) / baseline_latency_ms
        
        # Calculate latency stability (coefficient of variation)
        latency_stability = 0.0
        if avg_latency > 0:
            std_dev = statistics.stdev(latencies) if len(latencies) > 1 else 0.0
            latency_stability = 1.0 - (std_dev / avg_latency)  # Lower CV = higher stability
        
        return {
            'avg_latency_ms': avg_latency,
            'median_latency_ms': median_latency,
            'p95_latency_ms': p95_latency,
            'p99_latency_ms': p99_latency,
            'max_latency_ms': max_latency,
            'latency_improvement_ratio': latency_improvement_ratio,
            'latency_stability': max(0.0, latency_stability)
        }
    
    def calculate_quality_metrics(self, 
                                metrics: LoadSheddingMetrics, 
                                ground_truth_matches: Optional[int] = None) -> Dict[str, float]:
        """Calculate quality preservation metrics."""
        if ground_truth_matches is None or ground_truth_matches <= 0:
            # Use a heuristic if ground truth is not available
            # Assume we would have found matches proportional to processed events
            estimated_ground_truth = max(1, int(metrics.events_processed * 0.1))
        else:
            estimated_ground_truth = ground_truth_matches
        
        # Calculate precision, recall, and F1 score
        true_positives = metrics.patterns_matched
        false_negatives = max(0, estimated_ground_truth - true_positives)
        false_positives = metrics.false_positives
        
        precision = true_positives / max(true_positives + false_positives, 1)
        recall = true_positives / max(true_positives + false_negatives, 1)
        f1_score = 2 * (precision * recall) / max(precision + recall, 1)
        
        # Quality preservation ratio
        quality_preservation = true_positives / max(estimated_ground_truth, 1)
        
        # Pattern completeness score
        completeness_scores = list(metrics.pattern_completeness_ratios.values())
        avg_completeness = statistics.mean(completeness_scores) if completeness_scores else quality_preservation
        
        return {
            'precision': precision,
            'recall': recall,
            'f1_score': f1_score,
            'quality_preservation_ratio': quality_preservation,
            'pattern_completeness_avg': avg_completeness,
            'false_negative_rate': false_negatives / max(estimated_ground_truth, 1),
            'false_positive_rate': false_positives / max(true_positives + false_positives, 1)
        }
    
    def calculate_efficiency_metrics(self, metrics: LoadSheddingMetrics) -> Dict[str, float]:
        """Calculate efficiency metrics."""
        total_events = metrics.events_processed + metrics.events_dropped
        
        if total_events == 0:
            return {
                'processing_efficiency': 0.0,
                'resource_utilization': 0.0,
                'cost_benefit_ratio': 0.0,
                'load_shedding_effectiveness': 0.0
            }
        
        # Processing efficiency: ratio of processed events to total events
        processing_efficiency = metrics.events_processed / total_events
        
        # Resource utilization (based on snapshots)
        resource_stats = metrics.get_resource_utilization_stats()
        if resource_stats:
            cpu_util = resource_stats.get('cpu', {}).get('avg', 0.0)
            memory_util = resource_stats.get('memory', {}).get('avg', 0.0)
            resource_utilization = (cpu_util + memory_util) / 2.0
        else:
            resource_utilization = 0.0
        
        # Cost-benefit ratio: patterns found per event processed
        cost_benefit_ratio = metrics.patterns_matched / max(metrics.events_processed, 1)
        
        # Load shedding effectiveness: ratio of activations to recovery success
        load_shedding_effectiveness = 1.0
        if metrics.load_shedding_activations > 0:
            avg_recovery_time = (sum(metrics.recovery_times) / len(metrics.recovery_times) 
                               if metrics.recovery_times else 0.0)
            # Effectiveness decreases with longer recovery times
            load_shedding_effectiveness = max(0.0, 1.0 - (avg_recovery_time / 60.0))  # Normalize by 1 minute
        
        return {
            'processing_efficiency': processing_efficiency,
            'resource_utilization': resource_utilization,
            'cost_benefit_ratio': cost_benefit_ratio,
            'load_shedding_effectiveness': load_shedding_effectiveness
        }
    
    def calculate_stability_metrics(self, metrics: LoadSheddingMetrics) -> Dict[str, float]:
        """Calculate system stability metrics."""
        if not metrics.snapshots:
            return {
                'throughput_stability': 0.0,
                'latency_stability': 0.0,
                'load_level_stability': 0.0,
                'overall_stability': 0.0
            }
        
        # Throughput stability - coefficient of variation of throughput over time
        throughputs = [snapshot.throughput_eps for snapshot in metrics.snapshots]
        throughput_stability = self._calculate_stability_coefficient(throughputs)
        
        # Latency stability - coefficient of variation of latency over time
        latencies = [snapshot.avg_latency_ms for snapshot in metrics.snapshots]
        latency_stability = self._calculate_stability_coefficient(latencies)
        
        # Load level stability - frequency of load level changes
        load_levels = [snapshot.load_level for snapshot in metrics.snapshots]
        load_changes = sum(1 for i in range(1, len(load_levels)) if load_levels[i] != load_levels[i-1])
        load_level_stability = 1.0 - (load_changes / max(len(load_levels) - 1, 1))
        
        # Overall stability score
        overall_stability = (throughput_stability + latency_stability + load_level_stability) / 3.0
        
        return {
            'throughput_stability': throughput_stability,
            'latency_stability': latency_stability,
            'load_level_stability': load_level_stability,
            'overall_stability': overall_stability
        }
    
    def _calculate_stability_coefficient(self, values: List[float]) -> float:
        """Calculate stability coefficient (1 - coefficient of variation)."""
        if not values or len(values) < 2:
            return 1.0
        
        mean_val = statistics.mean(values)
        if mean_val == 0:
            return 1.0
        
        std_dev = statistics.stdev(values)
        cv = std_dev / mean_val
        
        # Return stability score (higher is more stable)
        return max(0.0, 1.0 - cv)
    
    def _calculate_overall_score(self, 
                               throughput_metrics: Dict[str, float],
                               latency_metrics: Dict[str, float],
                               quality_metrics: Dict[str, float],
                               efficiency_metrics: Dict[str, float]) -> float:
        """
        Calculate overall performance score.
        
        This combines multiple metrics into a single score for easy comparison.
        """
        # Weights for different aspects (should sum to 1.0)
        weights = {
            'throughput': 0.25,
            'latency': 0.25,
            'quality': 0.30,
            'efficiency': 0.20
        }
        
        # Normalize metrics to 0-1 scale and calculate weighted score
        throughput_score = throughput_metrics.get('processing_efficiency', 0.0)
        
        # For latency, lower is better, so invert the score
        avg_latency = latency_metrics.get('avg_latency_ms', 1000.0)
        latency_score = max(0.0, 1.0 - (avg_latency / 1000.0))  # Normalize by 1 second
        
        quality_score = quality_metrics.get('f1_score', 0.0)
        efficiency_score = efficiency_metrics.get('processing_efficiency', 0.0)
        
        overall_score = (
            weights['throughput'] * throughput_score +
            weights['latency'] * latency_score +
            weights['quality'] * quality_score +
            weights['efficiency'] * efficiency_score
        )
        
        return min(1.0, max(0.0, overall_score))
    
    def compare_strategies(self, evaluations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compare multiple strategy evaluations.
        
        Args:
            evaluations: List of evaluation results from evaluate_single_test
            
        Returns:
            Dict containing comparative analysis
        """
        if not evaluations:
            return {'error': 'No evaluations provided for comparison'}
        
        # Extract strategy names
        strategy_names = [eval_data.get('strategy_name', f'Strategy_{i}') 
                         for i, eval_data in enumerate(evaluations)]
        
        comparison = {
            'strategies_compared': strategy_names,
            'comparison_timestamp': datetime.now().isoformat(),
            'rankings': {},
            'best_in_category': {},
            'summary_table': {}
        }
        
        # Categories to compare
        categories = {
            'overall_score': ('Overall Score', 'higher_better'),
            'throughput.events_per_second': ('Throughput (EPS)', 'higher_better'),
            'latency.avg_latency_ms': ('Avg Latency (ms)', 'lower_better'),
            'quality.f1_score': ('Quality (F1)', 'higher_better'),
            'efficiency.processing_efficiency': ('Efficiency', 'higher_better'),
            'stability.overall_stability': ('Stability', 'higher_better')
        }
        
        # Calculate rankings for each category
        for category, (display_name, direction) in categories.items():
            values = []
            for i, evaluation in enumerate(evaluations):
                value = self._get_nested_value(evaluation, category)
                values.append((i, strategy_names[i], value))
            
            # Sort based on direction
            reverse_sort = (direction == 'higher_better')
            values.sort(key=lambda x: x[2], reverse=reverse_sort)
            
            # Record rankings
            comparison['rankings'][category] = [
                {'rank': rank + 1, 'strategy': name, 'value': value}
                for rank, (idx, name, value) in enumerate(values)
            ]
            
            # Record best strategy in this category
            best = values[0]
            comparison['best_in_category'][category] = {
                'strategy': best[1],
                'value': best[2],
                'display_name': display_name
            }
        
        # Create summary table
        for i, strategy_name in enumerate(strategy_names):
            strategy_summary = {}
            for category, (display_name, _) in categories.items():
                value = self._get_nested_value(evaluations[i], category)
                rank = next(item['rank'] for item in comparison['rankings'][category] 
                          if item['strategy'] == strategy_name)
                strategy_summary[display_name] = {'value': value, 'rank': rank}
            
            comparison['summary_table'][strategy_name] = strategy_summary
        
        return comparison
    
    def _get_nested_value(self, data: Dict, path: str) -> float:
        """Get nested value from dictionary using dot notation."""
        keys = path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return 0.0
        
        return float(value) if isinstance(value, (int, float)) else 0.0
    
    def generate_performance_report(self, evaluation: Dict[str, Any]) -> str:
        """
        Generate a human-readable performance report.
        
        Args:
            evaluation: Result from evaluate_single_test
            
        Returns:
            String containing formatted report
        """
        report_lines = [
            "=== LOAD SHEDDING PERFORMANCE REPORT ===",
            f"Generated: {evaluation['evaluation_timestamp']}",
            f"Test Duration: {evaluation['test_duration_seconds']:.2f} seconds",
            "",
            "THROUGHPUT METRICS:",
            f"  Events/sec: {evaluation['throughput']['events_per_second']:.2f}",
            f"  Patterns/sec: {evaluation['throughput']['patterns_per_second']:.2f}",
            f"  Drop Rate: {evaluation['throughput']['drop_rate']:.1%}",
            f"  Processing Efficiency: {evaluation['throughput']['processing_efficiency']:.1%}",
            "",
            "LATENCY METRICS:",
            f"  Average: {evaluation['latency']['avg_latency_ms']:.2f} ms",
            f"  Median: {evaluation['latency']['median_latency_ms']:.2f} ms",
            f"  95th Percentile: {evaluation['latency']['p95_latency_ms']:.2f} ms",
            f"  Stability: {evaluation['latency']['latency_stability']:.1%}",
            "",
            "QUALITY METRICS:",
            f"  Precision: {evaluation['quality']['precision']:.1%}",
            f"  Recall: {evaluation['quality']['recall']:.1%}",
            f"  F1 Score: {evaluation['quality']['f1_score']:.3f}",
            f"  Quality Preservation: {evaluation['quality']['quality_preservation_ratio']:.1%}",
            "",
            "EFFICIENCY METRICS:",
            f"  Resource Utilization: {evaluation['efficiency']['resource_utilization']:.1%}",
            f"  Cost-Benefit Ratio: {evaluation['efficiency']['cost_benefit_ratio']:.3f}",
            f"  Load Shedding Effectiveness: {evaluation['efficiency']['load_shedding_effectiveness']:.1%}",
            "",
            "STABILITY METRICS:",
            f"  Overall Stability: {evaluation['stability']['overall_stability']:.1%}",
            f"  Throughput Stability: {evaluation['stability']['throughput_stability']:.1%}",
            f"  Latency Stability: {evaluation['stability']['latency_stability']:.1%}",
            "",
            f"OVERALL SCORE: {evaluation['overall_score']:.3f}",
            "=" * 50
        ]
        
        return "\n".join(report_lines)
    
    def export_evaluation_data(self, filepath: str, evaluation: Dict[str, Any]):
        """Export evaluation data to JSON file."""
        try:
            with open(filepath, 'w') as f:
                json.dump(evaluation, f, indent=2, default=str)
        except Exception as e:
            print(f"Error exporting evaluation data: {e}")
    
    def get_evaluation_history(self) -> List[Dict[str, Any]]:
        """Get history of all evaluations."""
        return self.evaluation_history.copy()
    
    def clear_history(self):
        """Clear evaluation history."""
        self.evaluation_history.clear()