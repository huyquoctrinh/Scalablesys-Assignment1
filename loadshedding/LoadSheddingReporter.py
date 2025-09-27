"""
Visualization and reporting utilities for load shedding analysis.
This module provides comprehensive reporting and visualization capabilities.
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from typing import Dict, List, Any, Optional
from datetime import datetime
import json
import csv
import os

from .LoadSheddingMetrics import LoadSheddingMetrics


class LoadSheddingReporter:
    """
    Generate comprehensive reports and visualizations for load shedding analysis.
    
    This class provides various reporting formats including text summaries,
    CSV exports, and visualizations.
    """
    
    def __init__(self, output_directory: str = "load_shedding_reports"):
        """
        Initialize the reporter.
        
        Args:
            output_directory: Directory to save reports and visualizations
        """
        self.output_directory = output_directory
        self.ensure_output_directory()
    
    def ensure_output_directory(self):
        """Ensure output directory exists."""
        os.makedirs(self.output_directory, exist_ok=True)
    
    def generate_comprehensive_report(self, 
                                    benchmark_results: Dict[str, Any],
                                    evaluation_results: List[Dict[str, Any]],
                                    report_name: str = "load_shedding_analysis") -> str:
        """
        Generate a comprehensive analysis report.
        
        Args:
            benchmark_results: Results from LoadSheddingBenchmark
            evaluation_results: Results from PerformanceEvaluator
            report_name: Name for the report files
            
        Returns:
            str: Path to the generated report file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"{report_name}_{timestamp}.txt"
        report_path = os.path.join(self.output_directory, report_filename)
        
        try:
            with open(report_path, 'w') as f:
                # Write report header
                f.write("=" * 80 + "\n")
                f.write("COMPREHENSIVE LOAD SHEDDING ANALYSIS REPORT\n")
                f.write("=" * 80 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Report Name: {report_name}\n\n")
                
                # Executive summary
                self._write_executive_summary(f, benchmark_results, evaluation_results)
                
                # Detailed benchmark results
                self._write_benchmark_section(f, benchmark_results)
                
                # Performance evaluation results
                self._write_evaluation_section(f, evaluation_results)
                
                # Recommendations
                self._write_recommendations_section(f, benchmark_results, evaluation_results)
                
                f.write("\n" + "=" * 80 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 80 + "\n")
        
        except Exception as e:
            print(f"Error generating comprehensive report: {e}")
            return ""
        
        print(f"Comprehensive report generated: {report_path}")
        return report_path
    
    def create_performance_visualizations(self, 
                                        metrics: LoadSheddingMetrics,
                                        output_prefix: str = "performance") -> List[str]:
        """
        Create performance visualization charts.
        
        Args:
            metrics: LoadSheddingMetrics object with collected data
            output_prefix: Prefix for output files
            
        Returns:
            List of paths to generated visualization files
        """
        generated_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # 1. Throughput over time
            if metrics.snapshots:
                plt.figure(figsize=(12, 6))
                timestamps = [snapshot.timestamp for snapshot in metrics.snapshots]
                throughputs = [snapshot.throughput_eps for snapshot in metrics.snapshots]
                
                plt.plot(timestamps, throughputs, 'b-', linewidth=2)
                plt.title('Throughput Over Time')
                plt.xlabel('Time')
                plt.ylabel('Events Per Second')
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                
                throughput_file = os.path.join(self.output_directory, 
                                             f"{output_prefix}_throughput_{timestamp}.png")
                plt.savefig(throughput_file, dpi=300, bbox_inches='tight')
                plt.close()
                generated_files.append(throughput_file)
            
            # 2. Latency distribution
            if metrics.processing_latencies:
                plt.figure(figsize=(10, 6))
                plt.hist(metrics.processing_latencies, bins=50, alpha=0.7, edgecolor='black')
                plt.title('Processing Latency Distribution')
                plt.xlabel('Latency (ms)')
                plt.ylabel('Frequency')
                plt.axvline(sum(metrics.processing_latencies) / len(metrics.processing_latencies), 
                           color='red', linestyle='--', label='Mean')
                plt.legend()
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                
                latency_file = os.path.join(self.output_directory, 
                                          f"{output_prefix}_latency_dist_{timestamp}.png")
                plt.savefig(latency_file, dpi=300, bbox_inches='tight')
                plt.close()
                generated_files.append(latency_file)
            
            # 3. System resources over time
            if metrics.snapshots:
                plt.figure(figsize=(12, 8))
                
                timestamps = [snapshot.timestamp for snapshot in metrics.snapshots]
                cpu_usage = [snapshot.cpu_percent for snapshot in metrics.snapshots]
                memory_usage = [snapshot.memory_percent for snapshot in metrics.snapshots]
                queue_sizes = [snapshot.queue_size for snapshot in metrics.snapshots]
                
                # Create subplots
                fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
                
                # CPU usage
                ax1.plot(timestamps, cpu_usage, 'r-', linewidth=2)
                ax1.set_ylabel('CPU Usage (%)')
                ax1.set_title('System Resource Utilization Over Time')
                ax1.grid(True, alpha=0.3)
                
                # Memory usage
                ax2.plot(timestamps, memory_usage, 'g-', linewidth=2)
                ax2.set_ylabel('Memory Usage (%)')
                ax2.grid(True, alpha=0.3)
                
                # Queue size
                ax3.plot(timestamps, queue_sizes, 'b-', linewidth=2)
                ax3.set_ylabel('Queue Size')
                ax3.set_xlabel('Time')
                ax3.grid(True, alpha=0.3)
                
                plt.tight_layout()
                
                resources_file = os.path.join(self.output_directory, 
                                            f"{output_prefix}_resources_{timestamp}.png")
                plt.savefig(resources_file, dpi=300, bbox_inches='tight')
                plt.close()
                generated_files.append(resources_file)
            
            # 4. Load shedding effectiveness
            if metrics.snapshots:
                plt.figure(figsize=(12, 6))
                
                timestamps = [snapshot.timestamp for snapshot in metrics.snapshots]
                load_levels = [snapshot.load_level for snapshot in metrics.snapshots]
                drop_rates = [(snapshot.events_dropped / max(snapshot.events_dropped + snapshot.events_processed, 1)) * 100 
                             for snapshot in metrics.snapshots]
                
                # Create numerical representation of load levels
                load_level_map = {'normal': 1, 'medium': 2, 'high': 3, 'critical': 4}
                load_values = [load_level_map.get(level.lower(), 0) for level in load_levels]
                
                fig, ax1 = plt.subplots(figsize=(12, 6))
                
                color = 'tab:red'
                ax1.set_xlabel('Time')
                ax1.set_ylabel('Load Level', color=color)
                ax1.plot(timestamps, load_values, color=color, linewidth=2)
                ax1.tick_params(axis='y', labelcolor=color)
                ax1.set_yticks([1, 2, 3, 4])
                ax1.set_yticklabels(['Normal', 'Medium', 'High', 'Critical'])
                
                ax2 = ax1.twinx()
                color = 'tab:blue'
                ax2.set_ylabel('Drop Rate (%)', color=color)
                ax2.plot(timestamps, drop_rates, color=color, linewidth=2)
                ax2.tick_params(axis='y', labelcolor=color)
                
                plt.title('Load Level vs Drop Rate Over Time')
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                
                effectiveness_file = os.path.join(self.output_directory, 
                                                f"{output_prefix}_effectiveness_{timestamp}.png")
                plt.savefig(effectiveness_file, dpi=300, bbox_inches='tight')
                plt.close()
                generated_files.append(effectiveness_file)
        
        except Exception as e:
            print(f"Error creating performance visualizations: {e}")
        
        return generated_files
    
    def create_strategy_comparison_chart(self, 
                                       comparison_results: Dict[str, Any],
                                       output_filename: str = "strategy_comparison") -> str:
        """
        Create a chart comparing different strategies.
        
        Args:
            comparison_results: Results from PerformanceEvaluator.compare_strategies
            output_filename: Name for the output file
            
        Returns:
            str: Path to generated chart file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_path = os.path.join(self.output_directory, f"{output_filename}_{timestamp}.png")
        
        try:
            strategies = comparison_results.get('strategies_compared', [])
            if not strategies:
                return ""
            
            # Extract metrics for radar chart
            metrics = ['Overall Score', 'Throughput (EPS)', 'Quality (F1)', 'Efficiency', 'Stability']
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Create bar chart for comparison
            x_pos = range(len(strategies))
            
            # Get overall scores for each strategy
            overall_scores = []
            for strategy in strategies:
                strategy_data = comparison_results.get('summary_table', {}).get(strategy, {})
                overall_score = strategy_data.get('Overall Score', {}).get('value', 0.0)
                overall_scores.append(overall_score)
            
            bars = ax.bar(x_pos, overall_scores, alpha=0.7)
            
            # Color bars based on performance
            for i, bar in enumerate(bars):
                score = overall_scores[i]
                if score >= 0.8:
                    bar.set_color('green')
                elif score >= 0.6:
                    bar.set_color('orange')
                else:
                    bar.set_color('red')
            
            ax.set_xlabel('Load Shedding Strategy')
            ax.set_ylabel('Overall Performance Score')
            ax.set_title('Load Shedding Strategy Comparison')
            ax.set_xticks(x_pos)
            ax.set_xticklabels(strategies, rotation=45, ha='right')
            ax.set_ylim(0, 1.0)
            ax.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for i, (bar, score) in enumerate(zip(bars, overall_scores)):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                       f'{score:.3f}', ha='center', va='bottom')
            
            plt.tight_layout()
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
        except Exception as e:
            print(f"Error creating strategy comparison chart: {e}")
            return ""
        
        return chart_path
    
    def export_metrics_to_csv(self, 
                            metrics: LoadSheddingMetrics,
                            output_filename: str = "metrics_export") -> str:
        """
        Export metrics data to CSV format.
        
        Args:
            metrics: LoadSheddingMetrics object
            output_filename: Name for the CSV file
            
        Returns:
            str: Path to generated CSV file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(self.output_directory, f"{output_filename}_{timestamp}.csv")
        
        try:
            with open(csv_path, 'w', newline='') as csvfile:
                # Export time series data
                if metrics.time_series_data:
                    fieldnames = metrics.time_series_data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(metrics.time_series_data)
                else:
                    # Export summary data if time series not available
                    summary = metrics.get_summary_report()
                    writer = csv.writer(csvfile)
                    writer.writerow(['Metric', 'Value'])
                    
                    def write_nested_dict(data, prefix=''):
                        for key, value in data.items():
                            if isinstance(value, dict):
                                write_nested_dict(value, f"{prefix}{key}.")
                            else:
                                writer.writerow([f"{prefix}{key}", str(value)])
                    
                    write_nested_dict(summary)
        
        except Exception as e:
            print(f"Error exporting metrics to CSV: {e}")
            return ""
        
        return csv_path
    
    def _write_executive_summary(self, f, benchmark_results: Dict[str, Any], 
                               evaluation_results: List[Dict[str, Any]]):
        """Write executive summary section."""
        f.write("EXECUTIVE SUMMARY\n")
        f.write("-" * 20 + "\n\n")
        
        if benchmark_results:
            f.write(f"Benchmark Type: {benchmark_results.get('test_type', 'Unknown')}\n")
            f.write(f"Test Duration: {benchmark_results.get('test_duration_seconds', 0)} seconds\n")
            f.write(f"Strategies Tested: {benchmark_results.get('strategies_tested', 0)}\n")
        
        if evaluation_results:
            f.write(f"Evaluations Completed: {len(evaluation_results)}\n")
            
            # Find best performing strategy
            best_strategy = max(evaluation_results, 
                              key=lambda x: x.get('overall_score', 0.0),
                              default={})
            
            if best_strategy:
                f.write(f"Best Performing Strategy: {best_strategy.get('strategy_name', 'Unknown')}\n")
                f.write(f"Best Overall Score: {best_strategy.get('overall_score', 0.0):.3f}\n")
        
        f.write("\n")
    
    def _write_benchmark_section(self, f, benchmark_results: Dict[str, Any]):
        """Write benchmark results section."""
        f.write("BENCHMARK RESULTS\n")
        f.write("-" * 20 + "\n\n")
        
        if not benchmark_results:
            f.write("No benchmark results available.\n\n")
            return
        
        results = benchmark_results.get('results', {})
        
        for strategy_name, strategy_results in results.items():
            f.write(f"Strategy: {strategy_name}\n")
            f.write("-" * len(f"Strategy: {strategy_name}") + "\n")
            
            for scenario, scenario_result in strategy_results.items():
                f.write(f"  {scenario}:\n")
                f.write(f"    Events Processed: {scenario_result.get('events_processed', 0)}\n")
                f.write(f"    Drop Rate: {scenario_result.get('drop_rate', 0.0):.1%}\n")
                f.write(f"    Avg Throughput: {scenario_result.get('avg_throughput_eps', 0.0):.2f} EPS\n")
                f.write(f"    Avg Latency: {scenario_result.get('avg_latency_ms', 0.0):.2f} ms\n")
            
            f.write("\n")
    
    def _write_evaluation_section(self, f, evaluation_results: List[Dict[str, Any]]):
        """Write performance evaluation section."""
        f.write("PERFORMANCE EVALUATION\n")
        f.write("-" * 20 + "\n\n")
        
        if not evaluation_results:
            f.write("No evaluation results available.\n\n")
            return
        
        for evaluation in evaluation_results:
            strategy_name = evaluation.get('strategy_name', 'Unknown Strategy')
            f.write(f"Strategy: {strategy_name}\n")
            f.write("-" * len(f"Strategy: {strategy_name}") + "\n")
            
            f.write(f"Overall Score: {evaluation.get('overall_score', 0.0):.3f}\n")
            f.write(f"Test Duration: {evaluation.get('test_duration_seconds', 0.0):.2f} seconds\n")
            
            # Throughput metrics
            throughput = evaluation.get('throughput', {})
            f.write(f"Throughput: {throughput.get('events_per_second', 0.0):.2f} EPS\n")
            f.write(f"Drop Rate: {throughput.get('drop_rate', 0.0):.1%}\n")
            
            # Latency metrics
            latency = evaluation.get('latency', {})
            f.write(f"Avg Latency: {latency.get('avg_latency_ms', 0.0):.2f} ms\n")
            f.write(f"P95 Latency: {latency.get('p95_latency_ms', 0.0):.2f} ms\n")
            
            # Quality metrics
            quality = evaluation.get('quality', {})
            f.write(f"F1 Score: {quality.get('f1_score', 0.0):.3f}\n")
            f.write(f"Quality Preservation: {quality.get('quality_preservation_ratio', 0.0):.1%}\n")
            
            f.write("\n")
    
    def _write_recommendations_section(self, f, benchmark_results: Dict[str, Any], 
                                     evaluation_results: List[Dict[str, Any]]):
        """Write recommendations section."""
        f.write("RECOMMENDATIONS\n")
        f.write("-" * 20 + "\n\n")
        
        if not evaluation_results:
            f.write("No evaluation data available for recommendations.\n\n")
            return
        
        # Find best strategies in different categories
        best_throughput = max(evaluation_results, 
                            key=lambda x: x.get('throughput', {}).get('events_per_second', 0.0),
                            default={})
        best_latency = min(evaluation_results,
                         key=lambda x: x.get('latency', {}).get('avg_latency_ms', float('inf')),
                         default={})
        best_quality = max(evaluation_results,
                         key=lambda x: x.get('quality', {}).get('f1_score', 0.0),
                         default={})
        
        f.write("Strategy Recommendations:\n")
        f.write(f"• For maximum throughput: {best_throughput.get('strategy_name', 'N/A')}\n")
        f.write(f"• For lowest latency: {best_latency.get('strategy_name', 'N/A')}\n")
        f.write(f"• For best quality preservation: {best_quality.get('strategy_name', 'N/A')}\n")
        
        f.write("\nGeneral Recommendations:\n")
        f.write("• Monitor system resources continuously during load shedding\n")
        f.write("• Adjust drop thresholds based on application requirements\n")
        f.write("• Consider semantic load shedding for quality-critical applications\n")
        f.write("• Use adaptive strategies for varying workload patterns\n")
        
        f.write("\n")
    
    def generate_summary_dashboard_data(self, 
                                      metrics: LoadSheddingMetrics,
                                      evaluation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate data for a summary dashboard.
        
        Returns:
            Dict containing dashboard-ready data
        """
        dashboard_data = {
            'timestamp': datetime.now().isoformat(),
            'test_duration': metrics.get_duration_seconds(),
            'overall_score': evaluation.get('overall_score', 0.0),
            'key_metrics': {
                'throughput_eps': evaluation.get('throughput', {}).get('events_per_second', 0.0),
                'avg_latency_ms': evaluation.get('latency', {}).get('avg_latency_ms', 0.0),
                'drop_rate_percent': evaluation.get('throughput', {}).get('drop_rate', 0.0) * 100,
                'quality_f1': evaluation.get('quality', {}).get('f1_score', 0.0),
                'efficiency_percent': evaluation.get('efficiency', {}).get('processing_efficiency', 0.0) * 100
            },
            'alerts': []
        }
        
        # Generate alerts based on thresholds
        if dashboard_data['key_metrics']['drop_rate_percent'] > 50:
            dashboard_data['alerts'].append("High drop rate detected (>50%)")
        
        if dashboard_data['key_metrics']['avg_latency_ms'] > 1000:
            dashboard_data['alerts'].append("High latency detected (>1000ms)")
        
        if dashboard_data['key_metrics']['quality_f1'] < 0.5:
            dashboard_data['alerts'].append("Low quality score detected (<0.5)")
        
        return dashboard_data