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
from typing import List, Iterator, Dict, Union
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import glob
# from condition.KleeneClosureCondition import KleeneClosureOperator, AdjacentChainingKC
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
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
from condition.KCCondition import KCCondition

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

class AdjacentChainingKC(KCCondition):
    def __init__(self, kleene_var="a",
                 bike_key="bikeid",
                 start_key="start_station_id",
                 end_key="end_station_id"):

        def _payload(ev):
            if isinstance(ev, dict):
                return ev
            p = getattr(ev, "payload", None)
            if p is not None:
                return p
            gp = getattr(ev, "get_payload", None)
            return gp() if callable(gp) else ev

        def _to_int(v):
            try:
                return int(v)
            except Exception:
                return v  # si no es convertible, lo dejamos tal cual

        def getattr_func(ev):
            p = _payload(ev)
            # tuple: (bike, start, end) con cast a int si es posible
            return (_to_int(p.get(bike_key)),
                    _to_int(p.get(start_key)),
                    _to_int(p.get(end_key)))

        def relation_op(prev_tuple, curr_tuple):
            # misma bici + encadenamiento
            return (prev_tuple[0] == curr_tuple[0]) and (prev_tuple[2] == curr_tuple[1])

        super().__init__(kleene_var, getattr_func, relation_op)

        self._kleene_var = kleene_var
        self._bike_key = bike_key
        self._start_key = start_key
        self._end_key = end_key

    def _to_payload(self, e):
        if isinstance(e, dict):
            return e
        p = getattr(e, "payload", None)
        if p is not None:
            return p
        gp = getattr(e, "get_payload", None)
        return gp() if callable(gp) else e

    def _extract_seq(self, ctx):
        if isinstance(ctx, list):
            return ctx
        if isinstance(ctx, dict):
            return ctx.get(self._kleene_var) or ctx.get("a") or []
        return []

    def _eval(self, ctx) -> bool:
        seq = self._extract_seq(ctx)
        if len(seq) <= 1:
            return True
        for prev, curr in zip(seq, seq[1:]):
            p = self._to_payload(prev)
            c = self._to_payload(curr)

            if str(p.get(self._bike_key)) != str(c.get(self._bike_key)):
                return False
            if str(p.get(self._end_key)) != str(c.get(self._start_key)):
                return False
        return True

class EnhancedCitiBikeStream(InputStream):
    """Enhanced stream with multi-threaded CSV processing and better importance calculation."""
    
    def __init__(self, csv_files: Union[str, List[str]], max_events: int = 10000, num_threads: int = 4):
        super().__init__()
        
        # Handle both single file and multiple files
        if isinstance(csv_files, str):
            # Support glob patterns
            if '*' in csv_files or '?' in csv_files:
                self.csv_files = glob.glob(csv_files)
            else:
                self.csv_files = [csv_files]
        else:
            self.csv_files = csv_files
        
        self.max_events = max_events
        self.num_threads = min(num_threads, len(self.csv_files))
        self.count = 0
        self.loading_complete = False
        
        # Thread-safe queue for processed events
        self.processed_queue = Queue()
        self.loading_lock = threading.Lock()
        
        # Station analysis for better importance calculation
        self.station_popularity = {}
        self.station_zones = {}
        
        # Load data with multi-threading
        self._load_data_multithreaded()
        
        print(f"Finished loading {self.count} events with enhanced attributes from {len(self.csv_files)} files")
    
    def _load_data_multithreaded(self):
        """Load data from multiple CSV files using multiple threads."""
        print(f"Loading enhanced CitiBike data from {len(self.csv_files)} files using {self.num_threads} threads...")
        
        # First pass: analyze stations across all files
        self._analyze_stations_multithreaded()
        
        # Second pass: load events with enhanced attributes
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            # Submit tasks for each CSV file
            future_to_file = {
                executor.submit(self._process_csv_file, csv_file, file_idx): csv_file 
                for file_idx, csv_file in enumerate(self.csv_files)
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                csv_file = future_to_file[future]
                try:
                    file_count = future.result()
                    print(f"Processed {file_count} events from {os.path.basename(csv_file)}")
                except Exception as e:
                    logger.error(f"Error processing {csv_file}: {e}")
        
        # Transfer processed events to main stream
        while not self.processed_queue.empty():
            try:
                event_data = self.processed_queue.get_nowait()
                self._stream.put(event_data)
                self.count += 1
            except Empty:
                break
        
        self.close()
    
    def _process_csv_file(self, csv_file: str, file_idx: int) -> int:
        """Process a single CSV file in a separate thread."""
        file_count = 0
        events_per_file = self.max_events // len(self.csv_files) if len(self.csv_files) > 1 else self.max_events
        
        try:
            formatter = CitiBikeCSVFormatter(csv_file)
            for data in formatter:
                with self.loading_lock:
                    if self.count >= self.max_events:
                        break
                
                if file_count >= events_per_file:
                    break
                
                # Enhanced importance calculation
                data['importance'] = self._calculate_enhanced_importance(data)
                data['priority'] = self._calculate_enhanced_priority(data)
                data['time_criticality'] = self._get_time_criticality(data)
                data['station_importance'] = self._get_station_importance(data)
                data['event_type'] = "BikeTrip"
                data['source_file'] = os.path.basename(csv_file)
                data['file_index'] = file_idx
                
                # Add to processed queue
                self.processed_queue.put(data)
                file_count += 1
                
                if file_count % 500 == 0:
                    print(f"Thread {file_idx}: Processed {file_count} events from {os.path.basename(csv_file)}")
                    
        except Exception as e:
            logger.error(f"Error in thread processing {csv_file}: {e}")
        
        return file_count
    
    def _analyze_stations_multithreaded(self):
        """Analyze station popularity across all files using multiple threads."""
        print("Analyzing stations across all files...")
        
        # Thread-safe collections for station data
        station_counts = {}
        station_locations = {}
        analysis_lock = threading.Lock()
        
        def analyze_file_stations(csv_file: str):
            """Analyze stations from a single file."""
            local_counts = {}
            local_locations = {}
            
            try:
                formatter = CitiBikeCSVFormatter(csv_file)
                sample_count = 0
                
                for data in formatter:
                    if sample_count >= 1000:  # Sample per file for analysis
                        break
                        
                    start_station = data.get("start_station_id")
                    end_station = data.get("end_station_id")
                    
                    if start_station:
                        local_counts[start_station] = local_counts.get(start_station, 0) + 1
                        if start_station not in local_locations:
                            local_locations[start_station] = {
                                'name': data.get("start_station_name", ""),
                                'lat': data.get("start_lat"),
                                'lng': data.get("start_lng")
                            }
                    
                    if end_station:
                        local_counts[end_station] = local_counts.get(end_station, 0) + 1
                        if end_station not in local_locations:
                            local_locations[end_station] = {
                                'name': data.get("end_station_name", ""),
                                'lat': data.get("end_lat"),
                                'lng': data.get("end_lng")
                            }
                    
                    sample_count += 1
                
                # Merge with global collections
                with analysis_lock:
                    for station_id, count in local_counts.items():
                        station_counts[station_id] = station_counts.get(station_id, 0) + count
                    
                    for station_id, location in local_locations.items():
                        if station_id not in station_locations:
                            station_locations[station_id] = location
                            
            except Exception as e:
                logger.error(f"Error analyzing stations in {csv_file}: {e}")
        
        # Process files in parallel for station analysis
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = [executor.submit(analyze_file_stations, csv_file) for csv_file in self.csv_files]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Station analysis error: {e}")
        
        # Calculate popularity scores (normalized)
        max_count = max(station_counts.values()) if station_counts else 1
        self.station_popularity = {
            station: count / max_count 
            for station, count in station_counts.items()
        }
        
        # Simple geographic zone classification
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
        
        print(f"Analyzed {len(self.station_popularity)} unique stations across {len(self.csv_files)} files")
    
    def get_thread_stats(self) -> Dict:
        """Get statistics about multi-threaded processing."""
        return {
            'total_files': len(self.csv_files),
            'threads_used': self.num_threads,
            'events_loaded': self.count,
            'stations_analyzed': len(self.station_popularity),
            'files_processed': [os.path.basename(f) for f in self.csv_files]
        }
    
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


def create_enhanced_patterns():
    logger.info("Creating sample patterns...")
    patterns = []

    # SEQ( BikeTrip+ a[], BikeTrip b )
    pattern1_structure = SeqOperator(
        KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a")),  # a[] with Kleene +
        PrimitiveEventStructure("BikeTrip", "b")                          # b
    )

    # chain a[]
    chain_inside_a = AdjacentChainingKC(
        kleene_var="a",
        bike_key="bikeid",
        start_key="start_station_id",
        end_key="end_station_id"
    )

    # same bike between a[last] y b
    same_bike_last_a_b = EqCondition(
        Variable("a", lambda x: x["bikeid"]),
        Variable("b", lambda x: x["bikeid"])
    )

    #b finish in {7,8,9}
    b_ends_in_target = AndCondition(
        GreaterThanCondition(Variable("b", lambda x: int(x["end_station_id"])), 6),
        SmallerThanCondition(Variable("b", lambda x: int(x["end_station_id"])), 10)
    )

    pattern1_condition = AndCondition(
        chain_inside_a,
        same_bike_last_a_b,
        b_ends_in_target
    )

    pattern1 = Pattern(
        pattern1_structure,
        pattern1_condition,
        timedelta(hours=1)  # 1 hour timeout
    )
    pattern1.name = "HotPathDetection"
    patterns.append(pattern1)

    logger.info(f"Created {len(patterns)} patterns")
    return patterns


class SimpleOutputStream(OutputStream):
    """Enhanced output stream with pattern analysis."""
    
    def __init__(self, file_path: str = "enhanced_citybike.txt", is_async: bool = False):
        super().__init__()
        self.file_path = file_path
        self.__is_async = is_async
        if self.__is_async:
            self.__output_file = open(self.file_path, 'w')
        else:
            self.__output_file = None
            self.__output_buffer = []
        self.matches = []
        self.pattern_stats = {}
    
    def add_item(self, item: object):
        """Add item with pattern analysis."""
        self.matches.append(item)
        super().add_item(item)
        
        if self.__is_async:
            self.__output_file.write(str(item) + "\n")
            self.__output_file.flush()  # Ensure immediate write
        else:
            self.__output_buffer.append(item)
        
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
    
    def get_matches(self):
        """Get all collected matches."""
        return self.matches
    
    def close(self):
        """Close the output stream and flush any pending data."""
        if self.__is_async and self.__output_file:
            self.__output_file.close()
            self.__output_file = None
        elif not self.__is_async and self.__output_buffer:
            # Write buffered items to file if needed
            if self.file_path:
                with open(self.file_path, 'w') as f:
                    for item in self.__output_buffer:
                        f.write(str(item) + "\n")
                self.__output_buffer.clear()
    
    def __del__(self):
        """Destructor to ensure file is closed."""
        if hasattr(self, '_SimpleOutputStream__is_async') and self.__is_async:
            if hasattr(self, '_SimpleOutputStream__output_file') and self.__output_file:
                try:
                    self.__output_file.close()
                except:
                    pass
    
class CitiBikeEventTypeClassifier:
    def get_event_type(self, event_payload):
        return "BikeTrip"

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

def run_enhanced_demo(csv_files: Union[str, List[str]], max_events: int = 5000, force_async: bool = False, num_threads: int = 4):
    """Run enhanced CitiBike load shedding demonstration with multi-file support."""
    
    # Handle file validation
    if isinstance(csv_files, str):
        if '*' in csv_files or '?' in csv_files:
            resolved_files = glob.glob(csv_files)
            if not resolved_files:
                print(f"Error: No files found matching pattern: {csv_files}")
                return
            csv_files = resolved_files
        else:
            if not os.path.exists(csv_files):
                print(f"Error: CSV file not found: {csv_files}")
                print(f"Current directory: {os.getcwd()}")
                print(f"Files in current directory: {[f for f in os.listdir('.') if f.endswith('.csv')]}")
                return
            csv_files = [csv_files]
    else:
        missing_files = [f for f in csv_files if not os.path.exists(f)]
        if missing_files:
            print(f"Error: Files not found: {missing_files}")
            return
    
    print("=" * 70)
    print("ENHANCED CITIBIKE MULTI-THREADED HOT PATH DETECTION & LOAD SHEDDING DEMO")
    print("=" * 70)
    print(f"Data files: {len(csv_files)} files")
    for i, f in enumerate(csv_files[:5]):  # Show first 5 files
        print(f"  {i+1}. {os.path.basename(f)}")
    if len(csv_files) > 5:
        print(f"  ... and {len(csv_files) - 5} more files")
    print(f"Max events: {max_events}")
    print(f"Threads: {num_threads}")
    print(f"Async mode: {force_async or max_events > 1000}")
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
        input_stream = EnhancedCitiBikeStream(csv_files, max_events, num_threads)
        # Enable async mode for better performance with large datasets or when explicitly requested
        is_async = force_async or max_events > 1000
        output_stream = SimpleOutputStream(f"enhanced_{config_name.replace(' ', '_').lower()}.txt", is_async=is_async)
        
        # Display thread statistics
        thread_stats = input_stream.get_thread_stats()
        print(f"  Thread stats: {thread_stats['threads_used']} threads, {thread_stats['total_files']} files, {thread_stats['stations_analyzed']} stations")
        
        # Simple data formatter
        
        data_formatter = SimpleCitiBikeDataFormatter()
        
        # Run processing
        print("  Processing events with enhanced patterns...")
        start_time = time.time()
        
        duration = cep.run(input_stream, output_stream, data_formatter)
        end_time = time.time()
        
        # Close output stream to ensure all data is written
        output_stream.close()
        
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
    
    parser = argparse.ArgumentParser(description='Enhanced Multi-threaded CitiBike Load Shedding Demo')
    parser.add_argument('--csv', default='test_data/201306-citibike-tripdata.csv',
                       help='Path to CitiBike CSV file(s). Supports glob patterns like "test_data/*.csv" or multiple files')
    parser.add_argument('--files', nargs='+',
                       help='Multiple CSV files to process')
    parser.add_argument('--events', type=int, default=2000,
                       help='Maximum number of events to process')
    parser.add_argument('--threads', type=int, default=4,
                       help='Number of threads for parallel processing')
    parser.add_argument('--async', action='store_true',
                       help='Enable async output mode for better performance')
    
    args = parser.parse_args()
    
    # Determine which files to process
    csv_files = args.files if args.files else args.csv
    
    print("Enhanced Multi-threaded CitiBike Hot Path Detection & Load Shedding Demo")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = run_enhanced_demo(csv_files, args.events, getattr(args, 'async'), args.threads)
    print("\nEnhanced multi-threaded demo completed successfully!")
    
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
