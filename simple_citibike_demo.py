#!/usr/bin/env python3
"""
Simple CitiBike Load Shedding Demo
This script demonstrates load shedding with real CitiBike data in a straightforward way.
"""

import os
import sys
import time
from datetime import datetime
from typing import List, Iterator

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from city_bike_formatter import CitiBikeCSVFormatter
from CEP import CEP
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter
from base.Event import Event
from base.Pattern import Pattern

# Load shedding imports
try:
    from loadshedding import LoadSheddingConfig, PresetConfigs
    LOAD_SHEDDING_AVAILABLE = True
except ImportError:
    LOAD_SHEDDING_AVAILABLE = False
    print("Load shedding not available")


class SimpleCitiBikeStream(InputStream):
    """Simple stream wrapper for CitiBike data."""
    
    def __init__(self, csv_file: str, max_events: int = 10000):
        self.formatter = CitiBikeCSVFormatter(csv_file)
        self.iterator = iter(self.formatter)
        self.max_events = max_events
        self.count = 0
    
    def get_item(self):
        if self.count >= self.max_events:
            raise StopIteration
        
        try:
            data = next(self.iterator)
            self.count += 1
            
            # Create simple event
            event = Event()
            event.timestamp = data["ts"].timestamp()
            event.payload = data
            
            # Add load shedding attributes
            if hasattr(event, '__dict__'):
                event.importance = self._get_importance(data)
                event.priority = self._get_priority(data)
                event.event_type = "trip"
            
            return event
            
        except StopIteration:
            raise
    
    def _get_importance(self, data):
        """Calculate importance for semantic load shedding."""
        importance = 0.5
        
        # Rush hour trips are more important
        hour = data["ts"].hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            importance += 0.3
        
        # Longer trips are more interesting
        if data["tripduration_s"] and data["tripduration_s"] > 1800:  # 30+ minutes
            importance += 0.2
        
        return min(1.0, importance)
    
    def _get_priority(self, data):
        """Calculate priority for semantic load shedding."""
        priority = 5.0
        
        # Subscribers have higher priority
        if data["usertype"] == "Subscriber":
            priority += 2.0
        
        # Weekday commutes are higher priority
        if data["ts"].weekday() < 5:  # Monday-Friday
            priority += 1.0
        
        return min(10.0, priority)
    
    def has_next(self):
        return self.count < self.max_events


class SimpleOutputStream(OutputStream):
    """Simple output stream to collect results."""
    
    def __init__(self):
        self.matches = []
    
    def add_item(self, item):
        self.matches.append(item)
    
    def get_matches(self):
        return self.matches


class SimpleCitiBikeDataFormatter(DataFormatter):
    """Simple data formatter for CitiBike events."""
    
    def format(self, data):
        return str(data)


def create_sample_patterns():
    """Create sample patterns for CitiBike analysis."""
    patterns = []
    
    # Pattern 1: Rush hour commute pattern
    pattern1 = Pattern()
    pattern1.name = "RushHourCommute"
    patterns.append(pattern1)
    
    # Pattern 2: Weekend leisure pattern
    pattern2 = Pattern()
    pattern2.name = "WeekendLeisure" 
    patterns.append(pattern2)
    
    # Pattern 3: Long distance trip pattern
    pattern3 = Pattern()
    pattern3.name = "LongDistanceTrip"
    patterns.append(pattern3)
    
    return patterns


def run_basic_citibike_test(csv_file: str, max_events: int = 5000):
    """Run basic CitiBike load shedding test."""
    
    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        return
    
    print("=" * 60)
    print("CITIBIKE LOAD SHEDDING DEMONSTRATION")
    print("=" * 60)
    print(f"Data file: {csv_file}")
    print(f"Max events: {max_events}")
    print(f"Load shedding available: {LOAD_SHEDDING_AVAILABLE}")
    
    # Create patterns
    patterns = create_sample_patterns()
    
    # Test configurations
    configs = {}
    
    if LOAD_SHEDDING_AVAILABLE:
        configs = {
            'No Load Shedding': LoadSheddingConfig(enabled=False),
            'Conservative': PresetConfigs.conservative(),
            'Semantic (CitiBike-tuned)': LoadSheddingConfig(
                strategy_name='semantic',
                pattern_priorities={
                    'RushHourCommute': 9.0,
                    'WeekendLeisure': 5.0,
                    'LongDistanceTrip': 7.0
                },
                importance_attributes=['importance', 'priority'],
                memory_threshold=0.7,
                cpu_threshold=0.8
            )
        }
    else:
        configs = {'No Load Shedding': None}
    
    results = {}
    
    for config_name, config in configs.items():
        print(f"\nTesting: {config_name}")
        print("-" * 40)
        
        try:
            # Create CEP instance
            if LOAD_SHEDDING_AVAILABLE and config:
                cep = CEP(patterns, load_shedding_config=config)
            else:
                cep = CEP(patterns)
            
            # Create streams
            input_stream = SimpleCitiBikeStream(csv_file, max_events)
            output_stream = SimpleOutputStream()
            data_formatter = SimpleCitiBikeDataFormatter()
            
            # Run processing
            print("  Processing events...")
            start_time = time.time()
            
            try:
                duration = cep.run(input_stream, output_stream, data_formatter)
                end_time = time.time()
                
                print(f"  ✓ Completed in {duration:.2f} seconds")
                print(f"  ✓ Wall clock time: {end_time - start_time:.2f} seconds")
                print(f"  ✓ Events processed: {input_stream.count}")
                print(f"  ✓ Matches found: {len(output_stream.get_matches())}")
                
                # Get load shedding statistics
                if cep.is_load_shedding_enabled():
                    stats = cep.get_load_shedding_statistics()
                    if stats:
                        print(f"  ✓ Load shedding strategy: {stats['strategy']}")
                        print(f"  ✓ Events dropped: {stats['events_dropped']}")
                        print(f"  ✓ Drop rate: {stats['drop_rate']:.1%}")
                        print(f"  ✓ Current load level: {stats.get('current_load_level', 'unknown')}")
                        print(f"  ✓ Average throughput: {stats.get('avg_throughput_eps', 0):.2f} EPS")
                        
                        results[config_name] = {
                            'success': True,
                            'duration': duration,
                            'wall_clock': end_time - start_time,
                            'events_processed': input_stream.count,
                            'matches_found': len(output_stream.get_matches()),
                            'load_shedding': stats
                        }
                    else:
                        print("  ! Load shedding statistics not available")
                else:
                    print("  ✓ Load shedding: Disabled")
                    results[config_name] = {
                        'success': True,
                        'duration': duration,
                        'wall_clock': end_time - start_time,
                        'events_processed': input_stream.count,
                        'matches_found': len(output_stream.get_matches()),
                        'load_shedding': None
                    }
                        
            except Exception as e:
                print(f"  ✗ Error during processing: {e}")
                results[config_name] = {'success': False, 'error': str(e)}
                
        except Exception as e:
            print(f"  ✗ Error creating CEP: {e}")
            results[config_name] = {'success': False, 'error': str(e)}
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    for config_name, result in results.items():
        print(f"\n{config_name}:")
        if result['success']:
            print(f"  Duration: {result['duration']:.2f}s")
            print(f"  Events: {result['events_processed']}")
            print(f"  Matches: {result['matches_found']}")
            
            if result['load_shedding']:
                print(f"  Drop Rate: {result['load_shedding']['drop_rate']:.1%}")
                print(f"  Strategy: {result['load_shedding']['strategy']}")
        else:
            print(f"  Error: {result['error']}")
    
    # Performance comparison
    if len([r for r in results.values() if r['success']]) > 1:
        print(f"\nPERFORMANCE COMPARISON:")
        print("-" * 25)
        
        successful_results = [(name, result) for name, result in results.items() if result['success']]
        baseline = successful_results[0][1]  # Use first successful as baseline
        
        for name, result in successful_results[1:]:
            if result['load_shedding']:
                drop_rate = result['load_shedding']['drop_rate']
                time_ratio = result['duration'] / baseline['duration']
                print(f"  {name}:")
                print(f"    Time vs baseline: {time_ratio:.2f}x")
                print(f"    Events dropped: {drop_rate:.1%}")
                print(f"    Matches preserved: {result['matches_found']}/{baseline['matches_found']}")
    
    return results


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='CitiBike Load Shedding Demo')
    parser.add_argument('--csv', default='201309-citibike-tripdata.csv',
                       help='Path to CitiBike CSV file')
    parser.add_argument('--events', type=int, default=10000,
                       help='Maximum number of events to process')
    
    args = parser.parse_args()
    
    # Check if file exists
    if not os.path.exists(args.csv):
        print(f"Error: CitiBike CSV file not found: {args.csv}")
        print("Please ensure the file exists and the path is correct.")
        print("Example: python simple_citibike_demo.py --csv /path/to/201309-citibike-tripdata.csv")
        return
    
    print("Simple CitiBike Load Shedding Demo")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        results = run_basic_citibike_test(args.csv, args.events)
        print("\nDemo completed successfully!")
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()