#!/usr/bin/env python3
"""
Enhanced CitiBike Load Shedding Demo with Improved Hot Path Detection
This script demonstrates the enhanced hot path detection and load shedding capabilities.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import List, Iterator, Dict
import statistics

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

try:
    from loadshedding.EnhancedLoadSheddingStrategy import (
        PatternAwareLoadShedding, 
        HybridAdaptiveLoadShedding,
        GeospatialLoadShedding
    )
    ENHANCED_STRATEGIES_AVAILABLE = True
except ImportError:
    ENHANCED_STRATEGIES_AVAILABLE = False
    logger.warning("Enhanced load shedding strategies not available")

LOAD_SHEDDING_AVAILABLE = True


class EnhancedCitiBikeStream(InputStream):
    """Enhanced stream with better importance calculation and station analysis."""
    
    def __init__(self, csv_file: str, max_events: int = 10000):
        super().__init__()
        self.formatter = CitiBikeCSVFormatter(csv_file)
        self.max_events = max_events
        self.count = 0
        
        # Station analysis for better importance calculation
        self.station_popularity = {}
        self.station_zones = {}
        self._analyze_stations()
        
        # Load data with enhanced attributes
        print(f"Loading enhanced CitiBike data from {csv_file}...")
        for data in self.formatter:
            if self.count >= max_events:
                break
            
            # Enhanced importance calculation
            data['importance'] = self._calculate_enhanced_importance(data)
            data['priority'] = self._calculate_enhanced_priority(data)
            data['time_criticality'] = self._get_time_criticality(data)
            data['station_importance'] = self._get_station_importance(data)
            data['event_type'] = "BikeTrip"
            
            self._stream.put(data)
            self.count += 1
            
            if self.count % 500 == 0:
                print(f"Loaded {self.count}/{max_events} events...")
        
        self.close()
        print(f"Finished loading {self.count} events with enhanced attributes")
    
    def _analyze_stations(self):
        """Analyze station popularity and geographic distribution."""
        station_counts = {}
        station_locations = {}
        
        # Quick pass to collect station statistics
        temp_formatter = CitiBikeCSVFormatter(self.formatter.filepath)
        sample_count = 0
        
        for data in temp_formatter:
            if sample_count >= 5000:  # Sample for analysis
                break
                
            start_station = data.get("start_station_id")
            end_station = data.get("end_station_id")
            
            if start_station:
                station_counts[start_station] = station_counts.get(start_station, 0) + 1
                if start_station not in station_locations:
                    station_locations[start_station] = {
                        'name': data.get("start_station_name", ""),
                        'lat': data.get("start_lat"),
                        'lng': data.get("start_lng")
                    }
            
            if end_station:
                station_counts[end_station] = station_counts.get(end_station, 0) + 1
                if end_station not in station_locations:
                    station_locations[end_station] = {
                        'name': data.get("end_station_name", ""),
                        'lat': data.get("end_lat"),
                        'lng': data.get("end_lng")
                    }
            
            sample_count += 1
        
        # Calculate popularity scores (normalized)
        max_count = max(station_counts.values()) if station_counts else 1
        self.station_popularity = {
            station: count / max_count 
            for station, count in station_counts.items()
        }
        
        # Simple geographic zone classification based on coordinates
        for station_id, location in station_locations.items():
            lat = location.get('lat', 0)
            lng = location.get('lng', 0)
            
            if lat and lng:
                # Simple Manhattan zones (this is approximate)
                if lat > 40.75:  # Upper Manhattan
                    zone = 'residential'
                elif lat > 40.72 and lng > -74.0:  # Midtown/Business
                    zone = 'business'
                elif lat < 40.71:  # Downtown/Financial
                    zone = 'business'
                else:
                    zone = 'tourist'
                
                self.station_zones[station_id] = zone
    
    def _calculate_enhanced_importance(self, data) -> float:
        """Enhanced importance calculation with multiple factors."""
        importance = 0.5  # Base importance
        
        # Time-based importance (more sophisticated)
        hour = data["ts"].hour
        weekday = data["ts"].weekday()
        
        if weekday < 5:  # Weekday
            if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hours
                importance += 0.4
            elif 6 <= hour <= 10 or 16 <= hour <= 20:  # Extended rush
                importance += 0.25
            elif 10 <= hour <= 16:  # Business hours
                importance += 0.15
        else:  # Weekend
            if 10 <= hour <= 18:  # Weekend activity hours
                importance += 0.2
        
        # Trip duration importance
        duration = data.get("tripduration_s", 0)
        if duration > 3600:  # Very long trips (>1 hour)
            importance += 0.25
        elif duration > 1800:  # Long trips (>30 minutes)
            importance += 0.15
        elif duration > 900:  # Medium trips (>15 minutes)
            importance += 0.1
        
        # Station popularity importance
        start_station = data.get("start_station_id")
        if start_station in self.station_popularity:
            importance += 0.2 * self.station_popularity[start_station]
        
        # User type importance
        if data.get("usertype") == "Subscriber":
            importance += 0.1
        
        # Distance-based importance (approximate)
        start_lat = data.get("start_lat", 0)
        start_lng = data.get("start_lng", 0)
        end_lat = data.get("end_lat", 0)
        end_lng = data.get("end_lng", 0)
        
        if all([start_lat, start_lng, end_lat, end_lng]):
            # Simple distance calculation
            lat_diff = abs(end_lat - start_lat)
            lng_diff = abs(end_lng - start_lng)
            distance_proxy = (lat_diff + lng_diff) * 111000  # Rough meters
            
            if distance_proxy > 3000:  # Long distance trips
                importance += 0.15
        
        return min(1.0, importance)
    
    def _calculate_enhanced_priority(self, data) -> float:
        """Enhanced priority calculation."""
        priority = 5.0  # Base priority
        
        # User type priority
        if data.get("usertype") == "Subscriber":
            priority += 2.5
        else:
            priority += 1.0  # Customer
        
        # Time-based priority
        hour = data["ts"].hour
        weekday = data["ts"].weekday()
        
        if weekday < 5:  # Weekday
            priority += 2.0
            if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hours
                priority += 2.0
        
        # Trip characteristics
        duration = data.get("tripduration_s", 0)
        if 600 <= duration <= 3600:  # Reasonable trip duration
            priority += 1.0
        
        # Station importance
        start_station = data.get("start_station_id")
        if start_station in self.station_popularity:
            priority += 2.0 * self.station_popularity[start_station]
        
        return min(10.0, priority)
    
    def _get_time_criticality(self, data) -> str:
        """Determine time criticality of the trip."""
        hour = data["ts"].hour
        weekday = data["ts"].weekday()
        
        if weekday < 5:  # Weekday
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                return "rush_hour"
            elif 6 <= hour <= 20:
                return "business_hours"
        else:  # Weekend
            if 10 <= hour <= 18:
                return "leisure_hours"
        
        return "off_peak"
    
    def _get_station_importance(self, data) -> float:
        """Get station importance score."""
        start_station = data.get("start_station_id")
        return self.station_popularity.get(start_station, 0.5)


def create_enhanced_patterns() -> List[Pattern]:
    """Create enhanced patterns for better hot path detection."""
    patterns = []
    
    # Pattern 1: Enhanced Morning Rush Hour Hot Paths
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
            SmallerThanCondition(Variable("morning_out", lambda x: x["ts"].weekday()), 5),  # Weekdays only
            # Add minimum trip duration to filter out very short trips
            GreaterThanCondition(Variable("morning_out", lambda x: x.get("tripduration_s", 0)), 300)
        ),
        7200  # 2 hours window for morning commute patterns
    )
    morning_pattern.name = "MorningRushHotPath"
    morning_pattern.priority = 9.0
    patterns.append(morning_pattern)
    
    # Pattern 2: Enhanced Evening Rush Hour Hot Paths
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
        7200  # 2 hours window
    )
    evening_pattern.name = "EveningRushHotPath"
    evening_pattern.priority = 9.0
    patterns.append(evening_pattern)
    
    # Pattern 3: Weekend Leisure Hot Paths
    weekend_pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("BikeTrip", "leisure_out"),
            PrimitiveEventStructure("BikeTrip", "leisure_return")
        ),
        AndCondition(
            EqCondition(Variable("leisure_out", lambda x: x["bikeid"]), 
                       Variable("leisure_return", lambda x: x["bikeid"])),
            EqCondition(Variable("leisure_out", lambda x: x["end_station_id"]), 
                       Variable("leisure_return", lambda x: x["start_station_id"])),
            GreaterThanCondition(Variable("leisure_out", lambda x: x["ts"].weekday()), 4),  # Weekends
            GreaterThanCondition(Variable("leisure_out", lambda x: x.get("tripduration_s", 0)), 600)  # Longer trips
        ),
        14400  # 4 hours window for leisure activities
    )
    weekend_pattern.name = "WeekendLeisureHotPath"
    weekend_pattern.priority = 6.0
    patterns.append(weekend_pattern)
    
    # Pattern 4: High-Value Station Hot Paths (for popular stations)
    popular_station_pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("BikeTrip", "pop_out"),
            PrimitiveEventStructure("BikeTrip", "pop_return")
        ),
        AndCondition(
            EqCondition(Variable("pop_out", lambda x: x["bikeid"]), 
                       Variable("pop_return", lambda x: x["bikeid"])),
            EqCondition(Variable("pop_out", lambda x: x["end_station_id"]), 
                       Variable("pop_return", lambda x: x["start_station_id"])),
            # Focus on trips involving popular stations
            GreaterThanCondition(Variable("pop_out", lambda x: x.get("station_importance", 0)), 0.7),
            GreaterThanCondition(Variable("pop_out", lambda x: x.get("tripduration_s", 0)), 300)
        ),
        5400  # 1.5 hours window
    )
    popular_station_pattern.name = "PopularStationHotPath"
    popular_station_pattern.priority = 7.5
    patterns.append(popular_station_pattern)
    
    return patterns


class SimpleOutputStream(OutputStream):
    """Enhanced output stream with pattern analysis."""
    
    def __init__(self, file_path: str = "enhanced_citybike.txt"):
        super().__init__()
        self.file_path = file_path
        self.matches = []
        self.pattern_stats = {}
    
    def add_item(self, item: object):
        """Add item with pattern analysis."""
        self.matches.append(item)
        super().add_item(item)
        
        # Analyze pattern if it's a pattern match
        if hasattr(item, 'pattern_name') or (isinstance(item, dict) and 'pattern' in str(item)):
            pattern_name = getattr(item, 'pattern_name', 'Unknown')
            if pattern_name not in self.pattern_stats:
                self.pattern_stats[pattern_name] = {
                    'count': 0,
                    'avg_importance': 0.0,
                    'avg_duration': 0.0,
                    'stations': set()
                }
            
            self.pattern_stats[pattern_name]['count'] += 1
    
    def get_analysis(self) -> Dict:
        """Get analysis of collected matches."""
        total_matches = len(self.matches)
        
        analysis = {
            'total_matches': total_matches,
            'patterns': self.pattern_stats,
            'match_rate': total_matches / 1000.0 if total_matches > 0 else 0.0
        }
        
        return analysis


def run_enhanced_demo(csv_file: str, max_events: int = 5000):
    """Run enhanced CitiBike load shedding demonstration."""
    
    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        return
    
    print("=" * 70)
    print("ENHANCED CITIBIKE HOT PATH DETECTION & LOAD SHEDDING DEMO")
    print("=" * 70)
    print(f"Data file: {csv_file}")
    print(f"Max events: {max_events}")
    print(f"Enhanced strategies available: {ENHANCED_STRATEGIES_AVAILABLE}")
    
    # Create enhanced patterns
    patterns = create_enhanced_patterns()
    print(f"Created {len(patterns)} enhanced patterns:")
    for pattern in patterns:
        print(f"  - {pattern.name} (priority: {getattr(pattern, 'priority', 'N/A')})")
    
    # Test configurations
    configs = {
        'No Load Shedding': LoadSheddingConfig(enabled=False),
        'Conservative Enhanced': LoadSheddingConfig(
            strategy_name='semantic',
            pattern_priorities={
                'MorningRushHotPath': 9.0,
                'EveningRushHotPath': 9.0,
                'PopularStationHotPath': 7.5,
                'WeekendLeisureHotPath': 6.0,
            },
            importance_attributes=['importance', 'priority', 'time_criticality', 'station_importance'],
            memory_threshold=0.75,
            cpu_threshold=0.85
        ),
        'Time-Aware Semantic': LoadSheddingConfig(
            strategy_name='semantic',
            pattern_priorities={
                'MorningRushHotPath': 10.0,  # Highest priority
                'EveningRushHotPath': 10.0,  # Highest priority
                'PopularStationHotPath': 8.0,
                'WeekendLeisureHotPath': 5.0,
            },
            importance_attributes=['importance', 'priority', 'time_criticality', 'station_importance'],
            memory_threshold=0.7,
            cpu_threshold=0.8
        )
    }
    
    results = {}
    
    for config_name, config in configs.items():
        print(f"\n{'-' * 50}")
        print(f"Testing: {config_name}")
        print(f"{'-' * 50}")
        
        # Create CEP instance
        if config.enabled:
            cep = CEP(patterns, load_shedding_config=config)
        else:
            cep = CEP(patterns)
        
        # Create streams
        input_stream = EnhancedCitiBikeStream(csv_file, max_events)
        output_stream = SimpleOutputStream(f"enhanced_{config_name.replace(' ', '_').lower()}.txt")
        
        # Simple data formatter
        class SimpleCitiBikeDataFormatter:
            def __init__(self):
                self.classifier = CitiBikeEventTypeClassifier()
            
            def get_event_type(self, event_payload):
                return self.classifier.get_event_type(event_payload)
            
            def get_probability(self, event_payload):
                # Return None for non-probabilistic events
                return None
            
            def parse_event(self, raw_data):
                return raw_data if isinstance(raw_data, dict) else {"data": str(raw_data)}
            
            def get_event_timestamp(self, event_payload):
                if "ts" in event_payload:
                    ts = event_payload["ts"]
                    return ts.timestamp() if hasattr(ts, 'timestamp') else float(ts)
                return datetime.now().timestamp()
            
            def format(self, data):
                return str(data)
        
        class CitiBikeEventTypeClassifier:
            def get_event_type(self, event_payload):
                return "BikeTrip"
        
        data_formatter = SimpleCitiBikeDataFormatter()
        
        # Run processing
        print("  Processing events with enhanced patterns...")
        start_time = time.time()
        
        duration = cep.run(input_stream, output_stream, data_formatter)
        end_time = time.time()
        
        # Get results
        analysis = output_stream.get_analysis()
        
        print(f"  ✓ Completed in {duration:.2f} seconds")
        print(f"  ✓ Wall clock time: {end_time - start_time:.2f} seconds")
        print(f"  ✓ Events processed: {input_stream.count}")
        print(f"  ✓ Total matches found: {analysis['total_matches']}")
        print(f"  ✓ Match rate: {analysis['match_rate']:.3f}")
        
        # Pattern-specific results
        for pattern_name, stats in analysis['patterns'].items():
            print(f"    - {pattern_name}: {stats['count']} matches")
        
        # Load shedding statistics
        if cep.is_load_shedding_enabled():
            ls_stats = cep.get_load_shedding_statistics()
            if ls_stats:
                print(f"  ✓ Load shedding strategy: {ls_stats['strategy']}")
                print(f"  ✓ Events dropped: {ls_stats['events_dropped']}")
                print(f"  ✓ Drop rate: {ls_stats['drop_rate']:.1%}")
                print(f"  ✓ Avg throughput: {ls_stats.get('avg_throughput_eps', 0):.1f} EPS")
        
        results[config_name] = {
            'duration': duration,
            'wall_clock': end_time - start_time,
            'events_processed': input_stream.count,
            'analysis': analysis,
            'load_shedding': cep.get_load_shedding_statistics() if cep.is_load_shedding_enabled() else None
        }
    
    # Summary comparison
    print(f"\n{'=' * 70}")
    print("ENHANCED DEMO SUMMARY & ANALYSIS")
    print(f"{'=' * 70}")
    
    for config_name, result in results.items():
        analysis = result['analysis']
        print(f"\n{config_name}:")
        print(f"  Duration: {result['duration']:.2f}s")
        print(f"  Total Matches: {analysis['total_matches']}")
        print(f"  Match Rate: {analysis['match_rate']:.3f}")
        
        if result['load_shedding']:
            ls = result['load_shedding']
            print(f"  Drop Rate: {ls['drop_rate']:.1%}")
            print(f"  Throughput: {ls.get('avg_throughput_eps', 0):.1f} EPS")
        
        # Pattern breakdown
        for pattern_name, stats in analysis['patterns'].items():
            print(f"    {pattern_name}: {stats['count']} matches")
    
    # Performance insights
    if len(results) > 1:
        print(f"\nPERFORMAN CE INSIGHTS:")
        print("-" * 30)
        
        baseline = list(results.values())[0]
        for config_name, result in list(results.items())[1:]:
            analysis = result['analysis']
            baseline_analysis = baseline['analysis']
            
            match_preservation = analysis['total_matches'] / max(baseline_analysis['total_matches'], 1)
            time_efficiency = baseline['duration'] / max(result['duration'], 0.01)
            
            print(f"{config_name}:")
            print(f"  Match Preservation: {match_preservation:.1%}")
            print(f"  Time Efficiency: {time_efficiency:.2f}x")
            
            if result['load_shedding']:
                drop_rate = result['load_shedding']['drop_rate']
                efficiency_score = match_preservation / max(1.0 - drop_rate, 0.1)
                print(f"  Efficiency Score: {efficiency_score:.2f}")
    
    return results


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced CitiBike Load Shedding Demo')
    parser.add_argument('--csv', default='201306-citibike-tripdata.csv',
                       help='Path to CitiBike CSV file')
    parser.add_argument('--events', type=int, default=2000,
                       help='Maximum number of events to process')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv):
        print(f"Error: CitiBike CSV file not found: {args.csv}")
        print("Please ensure the file exists and the path is correct.")
        return
    
    print("Enhanced CitiBike Hot Path Detection & Load Shedding Demo")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = run_enhanced_demo(args.csv, args.events)
    print("\nEnhanced demo completed successfully!")
    
    # Generate recommendations
    print(f"\n{'=' * 70}")
    print("RECOMMENDATIONS FOR PRODUCTION DEPLOYMENT")
    print(f"{'=' * 70}")
    
    print("1. Pattern Optimization:")
    print("   - Use time-aware patterns for better rush hour detection")
    print("   - Implement station popularity weighting")
    print("   - Add geographic zone-based filtering")
    
    print("\n2. Load Shedding Strategy:")
    print("   - Use semantic strategy during peak hours")
    print("   - Implement adaptive thresholds based on historical data")
    print("   - Configure pattern-specific importance weights")
    
    print("\n3. Performance Tuning:")
    print("   - Monitor pattern completion rates")
    print("   - Adjust time windows based on actual usage patterns")
    print("   - Implement predictive load monitoring")
    
    print("\n4. Quality Assurance:")
    print("   - Set minimum match rate thresholds")
    print("   - Monitor false positive rates")
    print("   - Implement pattern health scoring")


if __name__ == "__main__":
    main()
