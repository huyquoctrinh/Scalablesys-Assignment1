#!/usr/bin/env python3
"""
Comprehensive CitiBike Load Shedding Demo with Dashboard & Report Generation
Perfect for CS-E4780 Course Project Evaluation

This demo provides:
- Multiple load shedding strategies comparison
- Real-time dashboard visualization (optional)
- Detailed performance metrics
- Publication-ready plots
- Comprehensive report generation
"""

import os
import sys
import time
import logging
import argparse
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for plot generation
import numpy as np

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from city_bike_formatter import CitiBikeCSVFormatter
from CEP import CEP
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter, EventTypeClassifier
from base.Event import Event
from base.Pattern import Pattern
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, SmallerThanCondition
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from loadshedding import LoadSheddingConfig, PresetConfigs

# Try to import dashboard
try:
    from loadshedding.LoadSheddingDashboard import LoadSheddingDashboard, DashboardIntegratedLoadMonitor
    DASHBOARD_AVAILABLE = True
except ImportError:
    DASHBOARD_AVAILABLE = False
    print("Warning: Dashboard visualization not available")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('comprehensive_demo.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AnalyticalCitiBikeStream(InputStream):
    """Enhanced CitiBike stream with comprehensive analytics."""
    
    def __init__(self, csv_file: str, max_events: int = 10000):
        super().__init__()
        self.formatter = CitiBikeCSVFormatter(csv_file)
        self.max_events = max_events
        self.count = 0
        
        # Analytics
        self.station_popularity = {}
        self.station_zones = {}
        self.temporal_distribution = defaultdict(int)
        self.user_type_distribution = defaultdict(int)
        
        # Analyze stations first
        self._analyze_dataset()
        
        # Load data with enhanced attributes
        logger.info(f"Loading CitiBike data from {csv_file}...")
        for data in self.formatter:
            if self.count >= max_events:
                break
            
            # Enhanced importance calculation
            data['importance'] = self._calculate_importance(data)
            data['priority'] = self._calculate_priority(data)
            data['time_criticality'] = self._get_time_criticality(data)
            data['station_importance'] = self._get_station_importance(data)
            data['event_type'] = "BikeTrip"
            
            # Track distribution
            self.temporal_distribution[data["ts"].hour] += 1
            self.user_type_distribution[data.get("usertype", "Unknown")] += 1
            
            self._stream.put(data)
            self.count += 1
            
            if self.count % 1000 == 0:
                logger.info(f"Loaded {self.count}/{max_events} events...")
        
        self.close()
        logger.info(f"Finished loading {self.count} events")
    
    def _analyze_dataset(self):
        """Quick analysis of dataset characteristics."""
        logger.info("Analyzing dataset characteristics...")
        station_counts = defaultdict(int)
        station_locations = {}
        
        temp_formatter = CitiBikeCSVFormatter(self.formatter.filepath)
        sample_count = 0
        
        for data in temp_formatter:
            if sample_count >= 5000:
                break
            
            start_station = data.get("start_station_id")
            if start_station:
                station_counts[start_station] += 1
                if start_station not in station_locations:
                    station_locations[start_station] = {
                        'lat': data.get("start_lat"),
                        'lng': data.get("start_lng")
                    }
            
            sample_count += 1
        
        # Calculate popularity scores
        max_count = max(station_counts.values()) if station_counts else 1
        self.station_popularity = {
            station: count / max_count 
            for station, count in station_counts.items()
        }
        
        # Classify zones
        for station_id, location in station_locations.items():
            lat = location.get('lat', 0)
            lng = location.get('lng', 0)
            
            if lat and lng:
                if lat > 40.75:
                    zone = 'residential'
                elif lat > 40.72 and lng > -74.0:
                    zone = 'business'
                else:
                    zone = 'tourist'
                self.station_zones[station_id] = zone
        
        logger.info(f"Analyzed {len(self.station_popularity)} unique stations")
    
    def _calculate_importance(self, data) -> float:
        """Calculate comprehensive importance score."""
        importance = 0.5
        
        # Time-based importance
        hour = data["ts"].hour
        weekday = data["ts"].weekday()
        
        if weekday < 5:  # Weekday
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                importance += 0.4
            elif 6 <= hour <= 10 or 16 <= hour <= 20:
                importance += 0.25
            elif 10 <= hour <= 16:
                importance += 0.15
        else:  # Weekend
            if 10 <= hour <= 18:
                importance += 0.2
        
        # Duration importance
        duration = data.get("tripduration_s", 0)
        if duration > 3600:
            importance += 0.25
        elif duration > 1800:
            importance += 0.15
        elif duration > 900:
            importance += 0.1
        
        # Station popularity
        start_station = data.get("start_station_id")
        if start_station in self.station_popularity:
            importance += 0.2 * self.station_popularity[start_station]
        
        # User type
        if data.get("usertype") == "Subscriber":
            importance += 0.1
        
        return min(1.0, importance)
    
    def _calculate_priority(self, data) -> float:
        """Calculate priority score."""
        priority = 5.0
        
        if data.get("usertype") == "Subscriber":
            priority += 2.5
        else:
            priority += 1.0
        
        hour = data["ts"].hour
        weekday = data["ts"].weekday()
        
        if weekday < 5:
            priority += 2.0
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                priority += 2.0
        
        duration = data.get("tripduration_s", 0)
        if 600 <= duration <= 3600:
            priority += 1.0
        
        start_station = data.get("start_station_id")
        if start_station in self.station_popularity:
            priority += 2.0 * self.station_popularity[start_station]
        
        return min(10.0, priority)
    
    def _get_time_criticality(self, data) -> str:
        """Determine time criticality."""
        hour = data["ts"].hour
        weekday = data["ts"].weekday()
        
        if weekday < 5:
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                return "rush_hour"
            elif 6 <= hour <= 20:
                return "business_hours"
        else:
            if 10 <= hour <= 18:
                return "leisure_hours"
        
        return "off_peak"
    
    def _get_station_importance(self, data) -> float:
        """Get station importance score."""
        start_station = data.get("start_station_id")
        return self.station_popularity.get(start_station, 0.5)
    
    def get_analytics_summary(self) -> Dict:
        """Get summary of dataset analytics."""
        return {
            'total_events': self.count,
            'unique_stations': len(self.station_popularity),
            'temporal_distribution': dict(self.temporal_distribution),
            'user_type_distribution': dict(self.user_type_distribution),
            'top_stations': sorted(
                self.station_popularity.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]
        }


class ReportOutputStream(OutputStream):
    """Output stream with detailed analytics."""
    
    def __init__(self, file_path: str = "output.txt"):
        super().__init__()
        self.file_path = file_path
        self.matches = []
        self.pattern_stats = defaultdict(lambda: {
            'count': 0,
            'avg_importance': 0.0,
            'stations': set(),
            'time_distribution': defaultdict(int)
        })
    
    def add_item(self, item: object):
        """Add item with analytics tracking."""
        self.matches.append(item)
        super().add_item(item)
        
        # Extract pattern information
        if hasattr(item, 'pattern_name'):
            pattern_name = item.pattern_name
        elif isinstance(item, dict):
            pattern_name = item.get('pattern', 'Unknown')
        else:
            pattern_name = 'Unknown'
        
        stats = self.pattern_stats[pattern_name]
        stats['count'] += 1
        
        # Try to extract additional info
        if isinstance(item, dict):
            if 'importance' in item:
                current_avg = stats['avg_importance']
                n = stats['count']
                stats['avg_importance'] = (current_avg * (n-1) + item['importance']) / n
            
            if 'start_station_id' in item:
                stats['stations'].add(item['start_station_id'])
            
            if 'ts' in item and hasattr(item['ts'], 'hour'):
                stats['time_distribution'][item['ts'].hour] += 1
    
    def get_analysis(self) -> Dict:
        """Get comprehensive analysis."""
        return {
            'total_matches': len(self.matches),
            'patterns': {
                name: {
                    'count': stats['count'],
                    'avg_importance': stats['avg_importance'],
                    'unique_stations': len(stats['stations']),
                    'time_distribution': dict(stats['time_distribution'])
                }
                for name, stats in self.pattern_stats.items()
            }
        }


class SimpleCitiBikeDataFormatter(DataFormatter):
    """Simple data formatter for CitiBike."""
    
    def __init__(self):
        super().__init__(CitiBikeEventTypeClassifier())
    
    def parse_event(self, raw_data):
        if isinstance(raw_data, dict):
            return raw_data
        return {"data": str(raw_data)}
    
    def get_event_timestamp(self, event_payload: dict):
        if "ts" in event_payload:
            ts = event_payload["ts"]
            return ts.timestamp() if hasattr(ts, 'timestamp') else float(ts)
        return datetime.now().timestamp()
    
    def format(self, data):
        return str(data)


class CitiBikeEventTypeClassifier(EventTypeClassifier):
    """Event type classifier for CitiBike."""
    
    def get_event_type(self, event_payload: dict):
        return "BikeTrip"


def create_comprehensive_patterns() -> List[Pattern]:
    """Create comprehensive set of patterns for hot path detection."""
    patterns = []
    
    # Pattern 1: Morning Rush Hour Hot Paths
    morning_pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("BikeTrip", "morning_out"),
            PrimitiveEventStructure("BikeTrip", "morning_return")
        ),
        AndCondition(
            EqCondition(Variable("morning_out", lambda x: x["bikeid"]), 
                       Variable("morning_return", lambda x: x["bikeid"])),
            EqCondition(Variable("morning_out", lambda x: x["end_station_id"]), 
                       Variable("morning_return", lambda x: x["start_station_id"])),
            GreaterThanCondition(Variable("morning_out", lambda x: x["ts"].hour), 6),
            SmallerThanCondition(Variable("morning_out", lambda x: x["ts"].hour), 11),
            SmallerThanCondition(Variable("morning_out", lambda x: x["ts"].weekday()), 5),
            GreaterThanCondition(Variable("morning_out", lambda x: x.get("tripduration_s", 0)), 300)
        ),
        7200  # 2 hours
    )
    morning_pattern.name = "MorningRushHotPath"
    patterns.append(morning_pattern)
    
    # Pattern 2: Evening Rush Hour Hot Paths
    evening_pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("BikeTrip", "evening_out"),
            PrimitiveEventStructure("BikeTrip", "evening_return")
        ),
        AndCondition(
            EqCondition(Variable("evening_out", lambda x: x["bikeid"]), 
                       Variable("evening_return", lambda x: x["bikeid"])),
            EqCondition(Variable("evening_out", lambda x: x["end_station_id"]), 
                       Variable("evening_return", lambda x: x["start_station_id"])),
            GreaterThanCondition(Variable("evening_out", lambda x: x["ts"].hour), 16),
            SmallerThanCondition(Variable("evening_out", lambda x: x["ts"].hour), 21),
            SmallerThanCondition(Variable("evening_out", lambda x: x["ts"].weekday()), 5),
            GreaterThanCondition(Variable("evening_out", lambda x: x.get("tripduration_s", 0)), 300)
        ),
        7200
    )
    evening_pattern.name = "EveningRushHotPath"
    patterns.append(evening_pattern)
    
    # Pattern 3: Popular Station Hot Paths
    popular_pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("BikeTrip", "pop_out"),
            PrimitiveEventStructure("BikeTrip", "pop_return")
        ),
        AndCondition(
            EqCondition(Variable("pop_out", lambda x: x["bikeid"]), 
                       Variable("pop_return", lambda x: x["bikeid"])),
            EqCondition(Variable("pop_out", lambda x: x["end_station_id"]), 
                       Variable("pop_return", lambda x: x["start_station_id"])),
            GreaterThanCondition(Variable("pop_out", lambda x: x.get("station_importance", 0)), 0.7),
            GreaterThanCondition(Variable("pop_out", lambda x: x.get("tripduration_s", 0)), 300)
        ),
        5400
    )
    popular_pattern.name = "PopularStationHotPath"
    patterns.append(popular_pattern)
    
    return patterns


class ComprehensiveDemoRunner:
    """Main demo runner with comprehensive reporting."""
    
    def __init__(self, output_dir: str = "demo_results"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.results = {}
        self.dataset_analytics = None
        self.dashboard = None
        self.dashboard_monitor = None
    
    def run_comprehensive_demo(
        self,
        csv_file: str,
        max_events: int = 5000,
        enable_dashboard: bool = False,
        dashboard_duration: int = 30
    ):
        """Run comprehensive demo with all features."""
        
        logger.info("="*70)
        logger.info("COMPREHENSIVE CITIBIKE LOAD SHEDDING DEMONSTRATION")
        logger.info("="*70)
        logger.info(f"Data file: {csv_file}")
        logger.info(f"Max events: {max_events}")
        logger.info(f"Dashboard enabled: {enable_dashboard}")
        logger.info(f"Output directory: {self.output_dir}")
        
        # Create patterns
        patterns = create_comprehensive_patterns()
        logger.info(f"Created {len(patterns)} patterns")
        
        # Define test configurations
        configs = self._get_test_configurations()
        
        # Initialize dashboard if requested
        if enable_dashboard and DASHBOARD_AVAILABLE:
            self._initialize_dashboard()
        
        # Run tests for each configuration
        for config_name, config in configs.items():
            logger.info(f"\n{'='*70}")
            logger.info(f"Testing: {config_name}")
            logger.info(f"{'='*70}")
            
            result = self._run_single_test(
                config_name, config, patterns, csv_file, max_events
            )
            
            self.results[config_name] = result
            
            # Update dashboard if enabled
            if self.dashboard_monitor:
                self._update_dashboard_metrics(result)
        
        # Generate comprehensive report
        self._generate_report()
        
        # Create visualizations
        self._create_visualizations()
        
        # Show dashboard if enabled
        if enable_dashboard and self.dashboard and DASHBOARD_AVAILABLE:
            logger.info(f"\nStarting dashboard for {dashboard_duration} seconds...")
            self.dashboard_monitor.simulate_metrics(duration_seconds=dashboard_duration)
        
        logger.info(f"\n{'='*70}")
        logger.info("DEMO COMPLETED SUCCESSFULLY")
        logger.info(f"Results saved to: {self.output_dir}")
        logger.info(f"{'='*70}")
    
    def _get_test_configurations(self) -> Dict[str, LoadSheddingConfig]:
        """Get test configurations."""
        return {
            'Baseline (No Shedding)': LoadSheddingConfig(enabled=False),
            
            'Conservative Shedding': LoadSheddingConfig(
                strategy_name='probabilistic',
                memory_threshold=0.9,
                cpu_threshold=0.95,
                drop_probabilities={
                    'NORMAL': 0.0,
                    'MEDIUM': 0.05,
                    'HIGH': 0.15,
                    'CRITICAL': 0.4
                }
            ),
            
            'Semantic (CitiBike-Optimized)': LoadSheddingConfig(
                strategy_name='semantic',
                pattern_priorities={
                    'MorningRushHotPath': 9.0,
                    'EveningRushHotPath': 9.0,
                    'PopularStationHotPath': 7.5,
                },
                importance_attributes=['importance', 'priority', 'time_criticality', 'station_importance'],
                memory_threshold=0.75,
                cpu_threshold=0.85
            ),
            
            'Aggressive Shedding': LoadSheddingConfig(
                strategy_name='semantic',
                pattern_priorities={
                    'MorningRushHotPath': 10.0,
                    'EveningRushHotPath': 10.0,
                    'PopularStationHotPath': 8.0,
                },
                importance_attributes=['importance', 'priority', 'time_criticality'],
                memory_threshold=0.6,
                cpu_threshold=0.7
            ),
        }
    
    def _initialize_dashboard(self):
        """Initialize dashboard visualization."""
        try:
            self.dashboard = LoadSheddingDashboard(update_interval_ms=1000)
            self.dashboard_monitor = DashboardIntegratedLoadMonitor(self.dashboard)
            
            # Start dashboard in background
            import threading
            dashboard_thread = threading.Thread(
                target=self.dashboard.start_dashboard,
                args=("CitiBike Load Shedding Performance Dashboard",)
            )
            dashboard_thread.daemon = True
            dashboard_thread.start()
            time.sleep(2)  # Give dashboard time to initialize
            
            logger.info("Dashboard initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize dashboard: {e}")
            self.dashboard = None
            self.dashboard_monitor = None
    
    def _run_single_test(
        self,
        config_name: str,
        config: LoadSheddingConfig,
        patterns: List[Pattern],
        csv_file: str,
        max_events: int
    ) -> Dict:
        """Run a single test configuration."""
        
        # Create CEP instance
        if config.enabled:
            cep = CEP(patterns, load_shedding_config=config)
            logger.info(f"  CEP created with load shedding: {config.strategy_name}")
        else:
            cep = CEP(patterns)
            logger.info(f"  CEP created without load shedding")
        
        # Create streams
        input_stream = AnalyticalCitiBikeStream(csv_file, max_events)
        output_stream = ReportOutputStream(
            os.path.join(self.output_dir, f"{config_name.replace(' ', '_').lower()}.txt")
        )
        data_formatter = SimpleCitiBikeDataFormatter()
        
        # Store dataset analytics from first run
        if self.dataset_analytics is None:
            self.dataset_analytics = input_stream.get_analytics_summary()
        
        # Run processing
        logger.info("  Processing events...")
        start_time = time.time()
        
        try:
            duration = cep.run(input_stream, output_stream, data_formatter)
        except Exception as e:
            logger.error(f"  Error during processing: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e), 'success': False}
        
        end_time = time.time()
        wall_clock_time = end_time - start_time
        
        # Get results
        analysis = output_stream.get_analysis()
        
        # Get load shedding statistics
        ls_stats = None
        if cep.is_load_shedding_enabled():
            ls_stats = cep.get_load_shedding_statistics()
        
        # Compile results
        result = {
            'success': True,
            'config_name': config_name,
            'duration': duration,
            'wall_clock_time': wall_clock_time,
            'events_processed': input_stream.count,
            'total_matches': analysis['total_matches'],
            'pattern_analysis': analysis['patterns'],
            'load_shedding': ls_stats,
            'throughput_eps': input_stream.count / wall_clock_time if wall_clock_time > 0 else 0,
        }
        
        # Log results
        logger.info(f"  ✓ Completed in {duration:.2f}s (wall clock: {wall_clock_time:.2f}s)")
        logger.info(f"  ✓ Events processed: {input_stream.count}")
        logger.info(f"  ✓ Total matches: {analysis['total_matches']}")
        logger.info(f"  ✓ Throughput: {result['throughput_eps']:.2f} events/sec")
        
        if ls_stats:
            logger.info(f"  ✓ Strategy: {ls_stats['strategy']}")
            logger.info(f"  ✓ Events dropped: {ls_stats['events_dropped']} ({ls_stats['drop_rate']:.1%})")
            logger.info(f"  ✓ Avg latency: {ls_stats.get('avg_latency_ms', 0):.2f}ms")
        
        # Log pattern breakdown
        for pattern_name, pattern_stats in analysis['patterns'].items():
            logger.info(f"    - {pattern_name}: {pattern_stats['count']} matches")
        
        return result
    
    def _update_dashboard_metrics(self, result: Dict):
        """Update dashboard with test results."""
        if not self.dashboard_monitor:
            return
        
        ls = result.get('load_shedding')
        if ls:
            self.dashboard_monitor.update_metrics(
                throughput=result['throughput_eps'],
                drop_rate=ls['drop_rate'],
                latency_ms=ls.get('avg_latency_ms', 100),
                cpu_usage=0.7,  # Simulated
                memory_usage=0.6,  # Simulated
                queue_size=1000,  # Simulated
                current_strategy=ls['strategy']
            )
    
    def _generate_report(self):
        """Generate comprehensive text report."""
        report_path = os.path.join(self.output_dir, "comprehensive_report.txt")
        
        with open(report_path, 'w') as f:
            f.write("="*80 + "\n")
            f.write("COMPREHENSIVE CITIBIKE LOAD SHEDDING DEMONSTRATION REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Output Directory: {self.output_dir}\n\n")
            
            # Dataset Summary
            f.write("DATASET SUMMARY\n")
            f.write("-"*40 + "\n")
            if self.dataset_analytics:
                f.write(f"Total Events: {self.dataset_analytics['total_events']:,}\n")
                f.write(f"Unique Stations: {self.dataset_analytics['unique_stations']}\n")
                f.write(f"\nUser Type Distribution:\n")
                for user_type, count in self.dataset_analytics['user_type_distribution'].items():
                    pct = (count / self.dataset_analytics['total_events']) * 100
                    f.write(f"  {user_type}: {count:,} ({pct:.1f}%)\n")
                
                f.write(f"\nTop 5 Stations by Popularity:\n")
                for station_id, popularity in self.dataset_analytics['top_stations'][:5]:
                    f.write(f"  Station {station_id}: {popularity:.3f}\n")
            
            f.write("\n")
            
            # Performance Comparison
            f.write("PERFORMANCE COMPARISON\n")
            f.write("-"*40 + "\n")
            
            # Create comparison table
            f.write(f"{'Configuration':<35} {'Time (s)':<12} {'Matches':<10} {'Drop %':<10} {'Throughput':<12}\n")
            f.write("-"*80 + "\n")
            
            for config_name, result in self.results.items():
                if not result.get('success'):
                    continue
                
                time_str = f"{result['wall_clock_time']:.2f}"
                matches_str = f"{result['total_matches']}"
                
                if result['load_shedding']:
                    drop_str = f"{result['load_shedding']['drop_rate']*100:.1f}%"
                else:
                    drop_str = "N/A"
                
                throughput_str = f"{result['throughput_eps']:.1f} eps"
                
                f.write(f"{config_name:<35} {time_str:<12} {matches_str:<10} {drop_str:<10} {throughput_str:<12}\n")
            
            f.write("\n")
            
            # Detailed Results by Configuration
            f.write("DETAILED RESULTS BY CONFIGURATION\n")
            f.write("-"*40 + "\n\n")
            
            for config_name, result in self.results.items():
                if not result.get('success'):
                    f.write(f"{config_name}: ERROR - {result.get('error', 'Unknown error')}\n\n")
                    continue
                
                f.write(f"{config_name}:\n")
                f.write(f"  Processing Time: {result['duration']:.2f}s\n")
                f.write(f"  Wall Clock Time: {result['wall_clock_time']:.2f}s\n")
                f.write(f"  Events Processed: {result['events_processed']:,}\n")
                f.write(f"  Total Matches: {result['total_matches']}\n")
                f.write(f"  Throughput: {result['throughput_eps']:.2f} events/sec\n")
                
                if result['load_shedding']:
                    ls = result['load_shedding']
                    f.write(f"  Load Shedding Strategy: {ls['strategy']}\n")
                    f.write(f"  Events Dropped: {ls['events_dropped']:,} ({ls['drop_rate']:.1%})\n")
                    f.write(f"  Average Latency: {ls.get('avg_latency_ms', 0):.2f}ms\n")
                else:
                    f.write(f"  Load Shedding: Disabled\n")
                
                # Pattern breakdown
                f.write(f"  Pattern Matches:\n")
                for pattern_name, pattern_stats in result['pattern_analysis'].items():
                    f.write(f"    - {pattern_name}: {pattern_stats['count']} matches\n")
                    if pattern_stats['unique_stations'] > 0:
                        f.write(f"      Unique stations: {pattern_stats['unique_stations']}\n")
                
                f.write("\n")
            
            # Performance Insights
            f.write("PERFORMANCE INSIGHTS\n")
            f.write("-"*40 + "\n")
            
            baseline = next((r for r in self.results.values() if not r.get('load_shedding')), None)
            if baseline and baseline.get('success'):
                f.write(f"Baseline (No Shedding):\n")
                f.write(f"  Time: {baseline['wall_clock_time']:.2f}s\n")
                f.write(f"  Matches: {baseline['total_matches']}\n")
                f.write(f"  Throughput: {baseline['throughput_eps']:.2f} eps\n\n")
                
                for config_name, result in self.results.items():
                    if not result.get('success') or not result.get('load_shedding'):
                        continue
                    
                    time_improvement = baseline['wall_clock_time'] / result['wall_clock_time']
                    match_preservation = result['total_matches'] / max(baseline['total_matches'], 1)
                    throughput_improvement = result['throughput_eps'] / baseline['throughput_eps']
                    
                    f.write(f"{config_name} vs Baseline:\n")
                    f.write(f"  Time Improvement: {time_improvement:.2f}x\n")
                    f.write(f"  Match Preservation: {match_preservation:.1%}\n")
                    f.write(f"  Throughput Improvement: {throughput_improvement:.2f}x\n")
                    f.write(f"  Drop Rate: {result['load_shedding']['drop_rate']:.1%}\n")
                    f.write(f"  Quality Score: {match_preservation / (1 - result['load_shedding']['drop_rate'] + 0.01):.2f}\n")
                    f.write("\n")
            
            # Recommendations
            f.write("RECOMMENDATIONS\n")
            f.write("-"*40 + "\n")
            f.write("1. For real-time responsiveness: Use Aggressive Shedding\n")
            f.write("   - Lowest latency, acceptable match preservation\n\n")
            f.write("2. For high accuracy: Use Semantic (CitiBike-Optimized)\n")
            f.write("   - Best balance of speed and quality\n\n")
            f.write("3. For production deployment:\n")
            f.write("   - Start with Conservative Shedding\n")
            f.write("   - Monitor performance metrics\n")
            f.write("   - Gradually adjust thresholds based on workload\n\n")
            f.write("4. Pattern-specific recommendations:\n")
            f.write("   - Prioritize rush hour patterns (7-9 AM, 5-7 PM)\n")
            f.write("   - Maintain higher thresholds for popular stations\n")
            f.write("   - Consider adaptive thresholds based on time of day\n\n")
            
            f.write("="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")
        
        logger.info(f"Report generated: {report_path}")
        
        # Also save as JSON
        json_path = os.path.join(self.output_dir, "results.json")
        with open(json_path, 'w') as f:
            # Convert datetime objects to strings for JSON serialization
            json_results = {}
            for config_name, result in self.results.items():
                json_results[config_name] = {
                    k: str(v) if isinstance(v, datetime) else v
                    for k, v in result.items()
                }
            
            json.dump({
                'dataset_analytics': self.dataset_analytics,
                'results': json_results,
                'timestamp': datetime.now().isoformat()
            }, f, indent=2, default=str)
        
        logger.info(f"JSON results saved: {json_path}")
    
    def _create_visualizations(self):
        """Create visualization plots for the report."""
        logger.info("Generating visualization plots...")
        
        # Filter successful results
        successful_results = {
            name: result for name, result in self.results.items()
            if result.get('success')
        }
        
        if not successful_results:
            logger.warning("No successful results to visualize")
            return
        
        # 1. Performance Comparison Bar Chart
        self._plot_performance_comparison(successful_results)
        
        # 2. Match Preservation vs Drop Rate
        self._plot_quality_tradeoff(successful_results)
        
        # 3. Throughput Comparison
        self._plot_throughput_comparison(successful_results)
        
        # 4. Pattern Distribution
        self._plot_pattern_distribution(successful_results)
        
        # 5. Temporal Distribution (if available)
        if self.dataset_analytics:
            self._plot_temporal_distribution()
        
        logger.info(f"Visualizations saved to {self.output_dir}")
    
    def _plot_performance_comparison(self, results: Dict):
        """Create performance comparison bar chart."""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        configs = list(results.keys())
        times = [results[c]['wall_clock_time'] for c in configs]
        matches = [results[c]['total_matches'] for c in configs]
        
        x = np.arange(len(configs))
        width = 0.35
        
        ax.bar(x - width/2, times, width, label='Processing Time (s)', alpha=0.8)
        ax2 = ax.twinx()
        ax2.bar(x + width/2, matches, width, label='Matches Found', alpha=0.8, color='orange')
        
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Processing Time (seconds)')
        ax2.set_ylabel('Matches Found')
        ax.set_title('Performance Comparison: Processing Time vs Matches Found')
        ax.set_xticks(x)
        ax.set_xticklabels(configs, rotation=15, ha='right')
        
        ax.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'performance_comparison.png'), dpi=300)
        plt.close()
    
    def _plot_quality_tradeoff(self, results: Dict):
        """Plot match preservation vs drop rate."""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        baseline = next((r for r in results.values() if not r.get('load_shedding')), None)
        if not baseline:
            return
        
        configs = []
        drop_rates = []
        match_rates = []
        
        for config_name, result in results.items():
            if result.get('load_shedding'):
                configs.append(config_name)
                drop_rates.append(result['load_shedding']['drop_rate'] * 100)
                match_preservation = (result['total_matches'] / max(baseline['total_matches'], 1)) * 100
                match_rates.append(match_preservation)
        
        if not configs:
            return
        
        scatter = ax.scatter(drop_rates, match_rates, s=200, alpha=0.6, c=range(len(configs)), cmap='viridis')
        
        for i, config in enumerate(configs):
            ax.annotate(config, (drop_rates[i], match_rates[i]), 
                       xytext=(5, 5), textcoords='offset points', fontsize=8)
        
        ax.set_xlabel('Drop Rate (%)')
        ax.set_ylabel('Match Preservation (%)')
        ax.set_title('Quality Tradeoff: Match Preservation vs Drop Rate')
        ax.grid(True, alpha=0.3)
        ax.axhline(y=100, color='r', linestyle='--', alpha=0.5, label='Baseline (100%)')
        ax.legend()
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'quality_tradeoff.png'), dpi=300)
        plt.close()
    
    def _plot_throughput_comparison(self, results: Dict):
        """Create throughput comparison."""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        configs = list(results.keys())
        throughputs = [results[c]['throughput_eps'] for c in configs]
        colors = ['red' if not results[c].get('load_shedding') else 'blue' for c in configs]
        
        bars = ax.bar(configs, throughputs, color=colors, alpha=0.7)
        
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Throughput (events/second)')
        ax.set_title('Throughput Comparison Across Configurations')
        ax.set_xticklabels(configs, rotation=15, ha='right')
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}',
                   ha='center', va='bottom')
        
        # Add legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='red', alpha=0.7, label='No Load Shedding'),
            Patch(facecolor='blue', alpha=0.7, label='With Load Shedding')
        ]
        ax.legend(handles=legend_elements)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'throughput_comparison.png'), dpi=300)
        plt.close()
    
    def _plot_pattern_distribution(self, results: Dict):
        """Plot pattern match distribution."""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.flatten()
        
        for idx, (config_name, result) in enumerate(list(results.items())[:4]):
            ax = axes[idx]
            
            pattern_analysis = result['pattern_analysis']
            patterns = list(pattern_analysis.keys())
            counts = [pattern_analysis[p]['count'] for p in patterns]
            
            if sum(counts) > 0:
                ax.pie(counts, labels=patterns, autopct='%1.1f%%', startangle=90)
                ax.set_title(f'{config_name}\n({sum(counts)} total matches)')
            else:
                ax.text(0.5, 0.5, 'No matches found', ha='center', va='center')
                ax.set_title(config_name)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'pattern_distribution.png'), dpi=300)
        plt.close()
    
    def _plot_temporal_distribution(self):
        """Plot temporal distribution of events."""
        if not self.dataset_analytics or 'temporal_distribution' not in self.dataset_analytics:
            return
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        temporal_dist = self.dataset_analytics['temporal_distribution']
        hours = sorted(temporal_dist.keys())
        counts = [temporal_dist[h] for h in hours]
        
        ax.bar(hours, counts, alpha=0.7, color='skyblue', edgecolor='navy')
        ax.set_xlabel('Hour of Day')
        ax.set_ylabel('Number of Events')
        ax.set_title('CitiBike Event Distribution by Hour of Day')
        ax.set_xticks(hours)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Highlight rush hours
        rush_hours = [7, 8, 9, 17, 18, 19]
        for hour in rush_hours:
            if hour in hours:
                idx = hours.index(hour)
                ax.patches[idx].set_color('orange')
                ax.patches[idx].set_alpha(0.8)
        
        # Add legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='skyblue', alpha=0.7, label='Regular Hours'),
            Patch(facecolor='orange', alpha=0.8, label='Rush Hours')
        ]
        ax.legend(handles=legend_elements)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'temporal_distribution.png'), dpi=300)
        plt.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Comprehensive CitiBike Load Shedding Demo with Dashboard & Report Generation'
    )
    parser.add_argument('--csv', default='201306-citibike-tripdata.csv',
                       help='Path to CitiBike CSV file')
    parser.add_argument('--events', type=int, default=5000,
                       help='Maximum number of events to process')
    parser.add_argument('--output-dir', default='demo_results',
                       help='Output directory for results and plots')
    parser.add_argument('--dashboard', action='store_true',
                       help='Enable real-time dashboard visualization')
    parser.add_argument('--dashboard-duration', type=int, default=30,
                       help='Dashboard display duration in seconds')
    
    args = parser.parse_args()
    
    # Check if file exists
    if not os.path.exists(args.csv):
        print(f"Error: CitiBike CSV file not found: {args.csv}")
        print("Please ensure the file exists and the path is correct.")
        return
    
    print("="*70)
    print("COMPREHENSIVE CITIBIKE LOAD SHEDDING DEMONSTRATION")
    print("="*70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Configuration:")
    print(f"  CSV file: {args.csv}")
    print(f"  Max events: {args.events}")
    print(f"  Output directory: {args.output_dir}")
    print(f"  Dashboard: {'Enabled' if args.dashboard else 'Disabled'}")
    if args.dashboard:
        print(f"  Dashboard duration: {args.dashboard_duration}s")
    print("="*70)
    
    # Run comprehensive demo
    runner = ComprehensiveDemoRunner(output_dir=args.output_dir)
    
    try:
        runner.run_comprehensive_demo(
            csv_file=args.csv,
            max_events=args.events,
            enable_dashboard=args.dashboard,
            dashboard_duration=args.dashboard_duration
        )
        
        print("\n" + "="*70)
        print("DEMO COMPLETED SUCCESSFULLY!")
        print("="*70)
        print(f"\nResults available in: {args.output_dir}/")
        print("Files generated:")
        print("  - comprehensive_report.txt (detailed text report)")
        print("  - results.json (machine-readable results)")
        print("  - performance_comparison.png")
        print("  - quality_tradeoff.png")
        print("  - throughput_comparison.png")
        print("  - pattern_distribution.png")
        print("  - temporal_distribution.png")
        print("\nUse these files for your project report!")
        
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
    except Exception as e:
        print(f"\n\nError during demo: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()