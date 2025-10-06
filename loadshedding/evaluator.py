# loadshedding/evaluator.py
"""
Evaluation framework for load shedding strategies.
Tests different latency targets and measures recall.
"""

import time
import logging
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class LoadSheddingEvaluator:
    """
    Evaluate load shedding effectiveness across different latency targets.
    Tests: 10%, 30%, 50%, 70%, 90% of baseline latency
    """
    
    def __init__(self, csv_file, max_events=5000):
        self.csv_file = csv_file
        self.max_events = max_events
        self.results = []
        
    def run_baseline(self):
        """Run without load shedding to get baseline metrics"""
        from simple_citibike_demo import create_sample_patterns, SimpleCitiBikeStream, SimpleOutputStream, SimpleCitiBikeDataFormatter
        from CEP import CEP
        
        print("\n" + "="*60)
        print("BASELINE RUN (No Load Shedding)")
        print("="*60)
        
        patterns = create_sample_patterns()
        cep = CEP(patterns)
        
        input_stream = SimpleCitiBikeStream(self.csv_file, self.max_events)
        output_stream = SimpleOutputStream("baseline_output.txt")
        data_formatter = SimpleCitiBikeDataFormatter()
        
        start_time = time.time()
        duration = cep.run(input_stream, output_stream, data_formatter)
        end_time = time.time()
        
        matches = len(output_stream.get_matches())
        
        baseline_metrics = {
            'config': 'baseline',
            'latency_target': 1.0,
            'duration': duration,
            'wall_clock': end_time - start_time,
            'events_processed': input_stream.count,
            'matches_found': matches,
            'recall': 1.0,
            'load_shedding': None
        }
        
        print(f"✓ Baseline Duration: {duration:.2f}s")
        print(f"✓ Baseline Matches: {matches}")
        
        return baseline_metrics
    
    def run_with_load_shedding(self, latency_target_ratio, baseline_latency):
        """
        Run with load shedding targeting a specific latency.
        
        Args:
            latency_target_ratio: Target latency as fraction of baseline
            baseline_latency: Baseline latency in seconds
            
        Returns:
            dict: Results including recall and statistics
        """
        from simple_citibike_demo import create_sample_patterns, SimpleCitiBikeStream, SimpleOutputStream, SimpleCitiBikeDataFormatter
        from CEP import CEP
        from loadshedding.config import LoadSheddingConfig
        
        print(f"\n" + "="*60)
        print(f"Target: {latency_target_ratio*100:.0f}% of baseline latency")
        print("="*60)
        
        # Adaptive thresholds
        if latency_target_ratio <= 0.3:
            memory_threshold = 0.5
            shedding_rate = 0.4
        elif latency_target_ratio <= 0.5:
            memory_threshold = 0.6
            shedding_rate = 0.3
        elif latency_target_ratio <= 0.7:
            memory_threshold = 0.7
            shedding_rate = 0.2
        else:
            memory_threshold = 0.8
            shedding_rate = 0.1
        
        config = LoadSheddingConfig(
            enabled=True,
            strategy_name='utility_based_hot_path',
            memory_threshold=memory_threshold,
            target_latency_ratio=latency_target_ratio,
            shedding_rate=shedding_rate
        )
        
        patterns = create_sample_patterns()
        cep = CEP(patterns, load_shedding_config=config)
        
        input_stream = SimpleCitiBikeStream(self.csv_file, self.max_events)
        output_stream = SimpleOutputStream(
            f"output_target_{int(latency_target_ratio*100)}.txt"
        )
        data_formatter = SimpleCitiBikeDataFormatter()
        
        start_time = time.time()
        duration = cep.run(input_stream, output_stream, data_formatter)
        end_time = time.time()
        
        matches = len(output_stream.get_matches())
        stats = cep.get_load_shedding_statistics() if cep.is_load_shedding_enabled() else {}
        
        print(f"✓ Duration: {duration:.2f}s ({duration/baseline_latency*100:.1f}% of baseline)")
        print(f"✓ Matches: {matches}")
        print(f"✓ Drop Rate: {stats.get('drop_rate', 0):.1%}")
        
        return {
            'config': f'{int(latency_target_ratio*100)}% latency',
            'latency_target': latency_target_ratio,
            'duration': duration,
            'wall_clock': end_time - start_time,
            'events_processed': input_stream.count,
            'matches_found': matches,
            'load_shedding': stats
        }
    
    def run_full_evaluation(self):
        """Run complete evaluation"""
        print("\n" + "🚀 "*30)
        print("FULL LOAD SHEDDING EVALUATION")
        print("🚀 "*30)
        
        baseline = self.run_baseline()
        self.results.append(baseline)
        
        latency_targets = [0.1, 0.3, 0.5, 0.7, 0.9]
        
        for target in latency_targets:
            result = self.run_with_load_shedding(target, baseline['duration'])
            result['recall'] = (result['matches_found'] / baseline['matches_found'] 
                               if baseline['matches_found'] > 0 else 0)
            self.results.append(result)
        
        self.print_summary(baseline['matches_found'], baseline['duration'])
        return self.results
    
    def print_summary(self, baseline_matches, baseline_latency):
        """Print evaluation summary"""
        print("\n" + "="*80)
        print("EVALUATION SUMMARY")
        print("="*80)
        print(f"{'Config':<20} {'Latency':<12} {'Recall':<10} {'Drop Rate':<12} {'Matches':<10}")
        print("-"*80)
        
        for r in self.results:
            config = r['config']
            latency_pct = (r['duration'] / baseline_latency * 100)
            recall = r['recall']
            drop_rate = r['load_shedding'].get('drop_rate', 0) if r['load_shedding'] else 0
            matches = r['matches_found']
            
            print(f"{config:<20} {latency_pct:>6.1f}% {recall:>8.1%} {drop_rate:>10.1%} {matches:>10}")
        
        print("="*80)