#!/usr/bin/env python3
"""
Enhanced Hot Path Demo with Advanced Load Shedding
This script demonstrates the complete hot path detection system with enhanced load shedding capabilities.
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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from city_bike_formatter import CitiBikeCSVFormatter
import csv
from CEP import CEP
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter, EventTypeClassifier
from base.Event import Event
from base.Pattern import Pattern
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, SmallerThanCondition
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from loadshedding import (
    LoadSheddingConfig, PresetConfigs, HotPathLoadSheddingStrategy, 
    LatencyAwareHotPathLoadShedding
)
from loadshedding.HotPathBenchmark import HotPathBenchmark
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


class EnhancedCitiBikeCSVFormatter:
    """Enhanced CitiBike CSV formatter that handles multiple datetime formats."""
    
    def __init__(self, csv_file: str):
        self.csv_file = csv_file
        
        # Multiple datetime formats to try
        self.datetime_formats = [
            '%Y-%m-%d %H:%M:%S.%f',  # 2018+ format with microseconds
            '%Y-%m-%d %H:%M:%S',     # 2013-2017 format
            '%m/%d/%Y %H:%M:%S',     # Alternative format
            '%m/%d/%Y %H:%M',        # Without seconds
        ]
    
    def __iter__(self):
        """Iterate through CSV data with enhanced datetime parsing."""
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                for row_num, row in enumerate(csv_reader, 1):
                    try:
                        # Clean and validate required fields
                        cleaned_row = self._clean_row_data(row)
                        
                        if self._is_valid_trip_data(cleaned_row):
                            yield cleaned_row
                        
                    except Exception as e:
                        logger.error(f"Error processing row {row_num} in {self.csv_file}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error loading CSV file {self.csv_file}: {e}")
    
    def _parse_datetime_flexible(self, datetime_str: str) -> datetime:
        """Parse datetime string with multiple format fallbacks."""
        if not datetime_str or datetime_str.strip() == '':
            return datetime.now()
        
        datetime_str = datetime_str.strip().strip('"')
        
        # Try each format until one works
        for fmt in self.datetime_formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue
        
        # If all formats fail, try to handle edge cases
        try:
            # Remove extra precision if present (more than 6 microsecond digits)
            if '.' in datetime_str:
                main_part, micro_part = datetime_str.rsplit('.', 1)
                if len(micro_part) > 6:
                    micro_part = micro_part[:6]  # Truncate to 6 digits
                datetime_str = f"{main_part}.{micro_part}"
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            pass
        
        # Last resort: return a default datetime
        logger.warning(f"Could not parse datetime '{datetime_str}', using current time")
        return datetime.now()
    
    def _clean_row_data(self, row: dict) -> dict:
        """Clean and standardize row data."""
        cleaned = {}
        
        # Handle different column name variations
        column_mappings = {
            'starttime': ['starttime', 'start_time', 'Start Time'],
            'stoptime': ['stoptime', 'stop_time', 'Stop Time'],
            'tripduration': ['tripduration', 'trip_duration', 'Trip Duration'],
            'start_station_id': ['start_station_id', 'start station id', 'Start Station ID'],
            'end_station_id': ['end_station_id', 'end station id', 'End Station ID'],
            'start_station_name': ['start_station_name', 'start station name', 'Start Station Name'],
            'end_station_name': ['end_station_name', 'end station name', 'End Station Name'],
            'bikeid': ['bikeid', 'bike_id', 'Bike ID'],
            'usertype': ['usertype', 'user_type', 'User Type']
        }
        
        for standard_key, possible_keys in column_mappings.items():
            for key in possible_keys:
                if key in row and row[key] is not None and str(row[key]).strip() != '':
                    cleaned[standard_key] = str(row[key]).strip().strip('"')
                    break
            else:
                # Set default values for missing fields
                if standard_key in ['starttime', 'stoptime']:
                    cleaned[standard_key] = datetime.now().isoformat()
                elif standard_key in ['start_station_id', 'end_station_id']:
                    cleaned[standard_key] = '-1'  # Invalid station ID
                elif standard_key == 'tripduration':
                    cleaned[standard_key] = '0'
                elif standard_key == 'bikeid':
                    cleaned[standard_key] = 'unknown'
                elif standard_key == 'usertype':
                    cleaned[standard_key] = 'unknown'
                else:
                    cleaned[standard_key] = ''
        
        # Parse timestamps and add to cleaned data
        try:
            cleaned['ts'] = self._parse_datetime_flexible(cleaned['starttime'])
            cleaned['tripduration_s'] = self._safe_int_convert(cleaned['tripduration'])
        except Exception as e:
            logger.error(f"Error parsing timestamps: {e}")
            cleaned['ts'] = datetime.now()
            cleaned['tripduration_s'] = 0
        
        return cleaned
    
    def _is_valid_trip_data(self, row: dict) -> bool:
        """Validate that row contains essential trip data."""
        required_fields = ['starttime', 'stoptime', 'bikeid']
        
        for field in required_fields:
            if field not in row or not row[field] or str(row[field]).strip() == '':
                return False
        
        # Additional validation - skip rows with invalid station IDs
        if (row.get('start_station_id', '-1') == '-1' and 
            row.get('end_station_id', '-1') == '-1'):
            return False
        
        return True
    
    def _safe_int_convert(self, value: str) -> int:
        """Safely convert string to int."""
        try:
            if not value or str(value).strip() == '' or str(value).lower() in ['null', 'none', '\\n']:
                return 0
            return int(float(value))  # Handle decimal strings
        except (ValueError, TypeError):
            return 0


class AdjacentChainingKC(KCCondition):
    """Kleene closure condition for bike chaining in hot path detection."""
    
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
                return v

        def getattr_func(ev):
            p = _payload(ev)
            return (_to_int(p.get(bike_key)),
                    _to_int(p.get(start_key)),
                    _to_int(p.get(end_key)))

        def relation_op(prev_tuple, curr_tuple):
            # Same bike + chaining: prev.end == curr.start
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
        # print(type(ctx))
        # print("CTX length:", len(ctx))
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


class EnhancedHotPathStream(InputStream):
    """Enhanced stream with hot path specific load shedding attributes."""
    
    def __init__(self, csv_files: Union[str, List[str]], max_events: int = 10000, num_threads: int = 4):
        super().__init__()
        
        # Handle both single file and multiple files
        if isinstance(csv_files, str):
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
        
        print(f"Finished loading {self.count} events with hot path attributes from {len(self.csv_files)} files")
    
    def _load_data_multithreaded(self):
        """Load data from multiple CSV files using multiple threads."""
        print(f"Loading hot path data from {len(self.csv_files)} files using {self.num_threads} threads...")
        
        # First pass: analyze stations across all files
        self._analyze_stations_multithreaded()
        
        # Second pass: load events with enhanced attributes
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_file = {
                executor.submit(self._process_csv_file, csv_file, file_idx): csv_file 
                for file_idx, csv_file in enumerate(self.csv_files)
            }
            
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
            formatter = EnhancedCitiBikeCSVFormatter(csv_file)
            for data in formatter:
                with self.loading_lock:
                    if self.count >= self.max_events:
                        break
                
                if file_count >= events_per_file:
                    break
                
                # Enhanced hot path importance calculation
                data['importance'] = self._calculate_hot_path_importance(data)
                data['priority'] = self._calculate_hot_path_priority(data)
                data['time_criticality'] = self._get_time_criticality(data)
                data['station_importance'] = self._get_station_importance(data)
                data['bike_chain_potential'] = self._calculate_bike_chain_potential(data)
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
        print("Analyzing stations for hot path detection...")
        
        station_counts = {}
        station_locations = {}
        analysis_lock = threading.Lock()
        
        def analyze_file_stations(csv_file: str):
            local_counts = {}
            local_locations = {}
            
            try:
                formatter = CitiBikeCSVFormatter(csv_file)
                sample_count = 0
                
                for data in formatter:
                    if sample_count >= 1000:
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
                
                with analysis_lock:
                    for station_id, count in local_counts.items():
                        station_counts[station_id] = station_counts.get(station_id, 0) + count
                    
                    for station_id, location in local_locations.items():
                        if station_id not in station_locations:
                            station_locations[station_id] = location
                            
            except Exception as e:
                logger.error(f"Error analyzing stations in {csv_file}: {e}")
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = [executor.submit(analyze_file_stations, csv_file) for csv_file in self.csv_files]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Station analysis error: {e}")
        
        # Calculate popularity scores
        max_count = max(station_counts.values()) if station_counts else 1
        self.station_popularity = {
            station: count / max_count 
            for station, count in station_counts.items()
        }
        
        print(f"Analyzed {len(self.station_popularity)} unique stations")
    
    def _calculate_hot_path_importance(self, data) -> float:
        """Calculate importance specifically for hot path detection."""
        importance = 0.5  # Base importance
        
        # Time-based importance (rush hours are critical for hot paths)
        hour = data["ts"].hour
        weekday = data["ts"].weekday()
        
        if weekday < 5:  # Weekday
            if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hours
                importance += 0.4
            elif 6 <= hour <= 10 or 16 <= hour <= 20:  # Extended rush
                importance += 0.25
        else:  # Weekend
            if 10 <= hour <= 18:  # Weekend activity
                importance += 0.2
        
        # Trip duration importance (longer trips indicate more complex patterns)
        duration = data.get("tripduration_s", 0)
        if duration > 3600:  # Very long trips
            importance += 0.25
        elif duration > 1800:  # Long trips
            importance += 0.15
        
        # Station importance (popular stations are more likely to be in hot paths)
        start_station = data.get("start_station_id")
        if start_station in self.station_popularity:
            importance += 0.2 * self.station_popularity[start_station]
        
        # Target station proximity (stations near 7,8,9 are more important)
        if start_station is not None and str(start_station).isdigit():
            station_id = int(start_station)
            if 6 < station_id < 10:  # Near target stations
                importance += 0.3
            elif 5 <= station_id <= 11:  # Close to target stations
                importance += 0.15
        
        # User type importance (subscribers are more predictable)
        if data.get("usertype") == "Subscriber":
            importance += 0.1
        
        return min(1.0, importance)
    
    def _calculate_hot_path_priority(self, data) -> float:
        """Calculate priority specifically for hot path detection."""
        priority = 5.0  # Base priority
        
        # User type priority
        if data.get("usertype") == "Subscriber":
            priority += 2.5
        else:
            priority += 1.0
        
        # Time-based priority (rush hours are highest priority)
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
        
        # Target station bonus
        if start_station is not None and str(start_station).isdigit():
            station_id = int(start_station)
            if 6 < station_id < 10:  # Target stations
                priority += 3.0
            elif 5 <= station_id <= 11:  # Near target stations
                priority += 1.5
        
        return min(10.0, priority)
    
    def _get_time_criticality(self, data) -> str:
        """Determine time criticality for hot path detection."""
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
    
    def _calculate_bike_chain_potential(self, data) -> float:
        """Calculate potential for this trip to be part of a bike chain."""
        potential = 0.5  # Base potential
        
        # Rush hour trips are more likely to be chained
        hour = data["ts"].hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            potential += 0.3
        
        # Subscriber trips are more likely to be chained
        if data.get("usertype") == "Subscriber":
            potential += 0.2
        
        # Popular stations are more likely to be chained
        start_station = data.get("start_station_id")
        if start_station in self.station_popularity:
            potential += 0.2 * self.station_popularity[start_station]
        
        # Target station proximity
        if start_station is not None and str(start_station).isdigit():
            station_id = int(start_station)
            if 6 < station_id < 10:
                potential += 0.3
        
        return min(1.0, potential)


def create_hot_path_patterns():
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

    # Safe function to handle NULL values in station IDs
    def safe_int_station_id(x):
        try:
            val = x.get("end_station_id", "-1")
            if val in ['NULL', 'null', '', None, '\\N']:
                return -1
            return int(val)
        except (ValueError, TypeError):
            return -1
    
    #b finish in {7,8,9}
    b_ends_in_target = AndCondition(
        GreaterThanCondition(Variable("b", safe_int_station_id), 6),
        SmallerThanCondition(Variable("b", safe_int_station_id), 10)
    )

    pattern1_condition = AndCondition(
        chain_inside_a,
        same_bike_last_a_b,
        b_ends_in_target
    )

    pattern1 = Pattern(
        pattern1_structure,
        pattern1_condition,
        timedelta(hours=1)
    )
    pattern1.name = "HotPathDetection"
    patterns.append(pattern1)

    logger.info(f"Created {len(patterns)} patterns")
    return patterns
# def create_hot_path_patterns():
#     """Create the hot path detection patterns as specified in the requirements."""
#     logger.info("Creating hot path patterns...")
#     patterns = []

#     # PATTERN SEQ( BikeTrip+ a[], BikeTrip b )
#     # WHERE a[i+1].bike = a[i].bike AND b.end in {7,8,9} 
#     # AND a[last].bike = b.bike AND a[i+1].start = a[i].end
#     # WITHIN 1h
#     pattern1_structure = SeqOperator(
#         KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a")),  # a[] with Kleene +
#         PrimitiveEventStructure("BikeTrip", "b")                          # b
#     )

#     # Chain a[] (adjacent trips by same bike)
#     chain_inside_a = AdjacentChainingKC(
#         kleene_var="a",
#         bike_key="bikeid",
#         start_key="start_station_id",
#         end_key="end_station_id"
#     )

#     def safe_int_end_station(x):
#         val = x.get("end_station_id")
#         try:
#             return int(val) if val is not None and str(val).isdigit() else -1
#         except Exception:
#             return -1
#     b_ends_in_target = AndCondition(
#         GreaterThanCondition(Variable("b", safe_int_end_station), 6),
#         SmallerThanCondition(Variable("b", safe_int_end_station), 10)
#     )
#     )

#     # b ends in {7,8,9}
#     b_ends_in_target = AndCondition(
#         GreaterThanCondition(Variable("b", lambda x: int(x["end_station_id"])), 6),
#         SmallerThanCondition(Variable("b", lambda x: int(x["end_station_id"])), 10)
#     )

#     pattern1_condition = AndCondition(
#         chain_inside_a,
#         same_bike_last_a_b,
#         b_ends_in_target
#     )

#     pattern1 = Pattern(
#         pattern1_structure,
#         pattern1_condition,
#         timedelta(hours=1)  # 1 hour window
#     )
#     pattern1.name = "HotPathDetection"
#     patterns.append(pattern1)

#     logger.info(f"Created {len(patterns)} hot path patterns")
#     return patterns


class HotPathOutputStream(OutputStream):
    """Output stream with hot path specific analysis."""
    
    def __init__(self, file_path: str = "hotpath_results.txt", is_async: bool = False):
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
        self.bike_chain_stats = {}
    
    def add_item(self, item: object):
        """Add item with hot path analysis."""
        self.matches.append(item)
        super().add_item(item)
        
        if self.__is_async:
            self.__output_file.write(str(item) + "\n")
            self.__output_file.flush()
        else:
            self.__output_buffer.append(item)

        # RETURN (a[1].start, a[i].end, b.end) extraction
        # Expecting AggregatedEvent for a[] and primitive Event for b
        try:
            if hasattr(item, 'events') and item.events and len(item.events) >= 2:
                a_event = item.events[0]
                b_event = item.events[1]
                a_prims = getattr(a_event, 'primitive_events', None)
                if isinstance(a_prims, list) and len(a_prims) > 0 and hasattr(b_event, 'payload'):
                    first_start = a_prims[0].payload.get("start_station_id")
                    last_end = a_prims[-1].payload.get("end_station_id")
                    b_end = b_event.payload.get("end_station_id")
                    if not self.__is_async:
                        self.__output_buffer.append(("RETURN", first_start, last_end, b_end))
                    else:
                        self.__output_file.write(str(("RETURN", first_start, last_end, b_end)) + "\n")
                        self.__output_file.flush()
        except Exception:
            pass
        
        # Analyze hot path pattern
        if hasattr(item, 'pattern_name') or (isinstance(item, dict) and 'pattern' in str(item)):
            pattern_name = getattr(item, 'pattern_name', 'HotPathDetection')
            if pattern_name not in self.pattern_stats:
                self.pattern_stats[pattern_name] = {
                    'count': 0,
                    'bike_chains': set(),
                    'target_stations': set(),
                    'avg_chain_length': 0.0
                }
            
            self.pattern_stats[pattern_name]['count'] += 1
            
            # Analyze bike chain if possible
            if hasattr(item, 'events') and item.events:
                self._analyze_bike_chain(item)
    
    def _analyze_bike_chain(self, match):
        """Analyze bike chain characteristics."""
        if not hasattr(match, 'events') or not match.events:
            return
        
        # Extract bike ID from first event
        first_event = match.events[0]
        if hasattr(first_event, 'payload') and 'bikeid' in first_event.payload:
            bike_id = first_event.payload['bikeid']
            self.bike_chain_stats[bike_id] = self.bike_chain_stats.get(bike_id, 0) + 1
    
    def get_analysis(self) -> Dict:
        """Get analysis of collected hot path matches."""
        total_matches = len(self.matches)
        
        analysis = {
            'total_matches': total_matches,
            'patterns': self.pattern_stats,
            'unique_bike_chains': len(self.bike_chain_stats),
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
            if self.file_path:
                with open(self.file_path, 'w') as f:
                    for item in self.__output_buffer:
                        f.write(str(item) + "\n")
                self.__output_buffer.clear()


class CitiBikeEventTypeClassifier:
    def get_event_type(self, event_payload):
        return "BikeTrip"


class HotPathDataFormatter:
    def __init__(self):
        self.classifier = CitiBikeEventTypeClassifier()
    
    def get_event_type(self, event_payload):
        return self.classifier.get_event_type(event_payload)
    
    def get_probability(self, event_payload):
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


def run_enhanced_hotpath_demo(csv_files: Union[str, List[str]], max_events: int = 5000, 
                            run_benchmark: bool = True, num_threads: int = 4):
    """Run enhanced hot path demonstration with comprehensive load shedding."""
    
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
                return
            csv_files = [csv_files]
    else:
        missing_files = [f for f in csv_files if not os.path.exists(f)]
        if missing_files:
            print(f"Error: Files not found: {missing_files}")
            return
    
    print("=" * 80)
    print("ENHANCED HOT PATH DETECTION WITH ADVANCED LOAD SHEDDING")
    print("=" * 80)
    print(f"Data files: {len(csv_files)} files")
    for i, f in enumerate(csv_files[:5]):
        print(f"  {i+1}. {os.path.basename(f)}")
    if len(csv_files) > 5:
        print(f"  ... and {len(csv_files) - 5} more files")
    print(f"Max events: {max_events}")
    print(f"Threads: {num_threads}")
    print(f"Benchmark: {run_benchmark}")
    
    # Create hot path patterns
    patterns = create_hot_path_patterns()
    
    # Test configurations
    configs = {
        'No Load Shedding': LoadSheddingConfig(enabled=False),
        'Hot Path Semantic': LoadSheddingConfig(
            strategy_name='semantic',
            pattern_priorities={
                'RushHourHotPath': 10.0,
                'SubscriberHotPath': 8.0,
                'PopularStationHotPath': 7.0,
                'LongDistanceHotPath': 6.0,
                'RegularHotPath': 5.0
            },
            importance_attributes=['importance', 'priority', 'time_criticality', 'station_importance', 'bike_chain_potential'],
            memory_threshold=0.75,
            cpu_threshold=0.85
        ),
        'Latency 50%': LoadSheddingConfig(
            strategy_name='adaptive',
            memory_threshold=0.7,
            cpu_threshold=0.8,
            latency_threshold_ms=50
        ),
        'Latency 30%': LoadSheddingConfig(
            strategy_name='adaptive',
            memory_threshold=0.6,
            cpu_threshold=0.7,
            latency_threshold_ms=30
        )
    }
    
    results = {}
    
    for config_name, config in configs.items():
        print(f"\n{'-' * 60}")
        print(f"Testing: {config_name}")
        print(f"{'-' * 60}")
        
        # Create CEP instance
        if config.enabled:
            cep = CEP(patterns, load_shedding_config=config)
        else:
            cep = CEP(patterns)
        
        # Create streams
        input_stream = EnhancedHotPathStream(csv_files, max_events, num_threads)
        is_async = max_events > 1000
        output_stream = HotPathOutputStream(f"hotpath_{config_name.replace(' ', '_').lower()}.txt", is_async=is_async)
        data_formatter = HotPathDataFormatter()
        
        # Run processing
        print("  Processing events with hot path patterns...")
        start_time = time.time()
        
        duration = cep.run(input_stream, output_stream, data_formatter)
        end_time = time.time()
        
        # Close output stream
        output_stream.close()
        
        # Get results
        analysis = output_stream.get_analysis()
        
        print(f"  ✓ Completed in {duration:.2f} seconds")
        print(f"  ✓ Wall clock time: {end_time - start_time:.2f} seconds")
        print(f"  ✓ Events processed: {input_stream.count}")
        print(f"  ✓ Hot path matches found: {analysis['total_matches']}")
        print(f"  ✓ Unique bike chains: {analysis['unique_bike_chains']}")
        print(f"  ✓ Match rate: {analysis['match_rate']:.3f}")
        
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
    
    # Run comprehensive benchmark if requested
    if run_benchmark:
        print(f"\n{'=' * 80}")
        print("RUNNING COMPREHENSIVE BENCHMARK")
        print(f"{'=' * 80}")
        
        benchmark = HotPathBenchmark()
        
        # Create fresh input stream for benchmark
        benchmark_input = EnhancedHotPathStream(csv_files, max_events, num_threads)
        benchmark_formatter = HotPathDataFormatter()
        
        # Run comprehensive benchmark
        benchmark_results = benchmark.run_comprehensive_benchmark(
            patterns, benchmark_input, benchmark_formatter, max_events, iterations=2
        )
        
        # Run recall-latency evaluation
        recall_results = benchmark.evaluate_recall_latency(
            patterns, benchmark_input, benchmark_formatter, max_events
        )
        
        print("Benchmark completed successfully!")
        print(f"Results saved to: {benchmark.output_dir}")
    
    # Summary comparison
    print(f"\n{'=' * 80}")
    print("HOT PATH DETECTION SUMMARY")
    print(f"{'=' * 80}")
    
    for config_name, result in results.items():
        analysis = result['analysis']
        print(f"\n{config_name}:")
        print(f"  Duration: {result['duration']:.2f}s")
        print(f"  Hot Path Matches: {analysis['total_matches']}")
        print(f"  Unique Bike Chains: {analysis['unique_bike_chains']}")
        print(f"  Match Rate: {analysis['match_rate']:.3f}")
        
        if result['load_shedding']:
            ls = result['load_shedding']
            print(f"  Drop Rate: {ls['drop_rate']:.1%}")
            print(f"  Throughput: {ls.get('avg_throughput_eps', 0):.1f} EPS")
    
    return results


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Hot Path Detection with Load Shedding')
    parser.add_argument('--csv', default='citybike_dataset/201306-citibike-tripdata.csv',
                       help='Path to CitiBike CSV file(s). Supports glob patterns')
    parser.add_argument('--files', nargs='+',
                       help='Multiple CSV files to process')
    parser.add_argument('--events', type=int, default=5000,
                       help='Maximum number of events to process')
    parser.add_argument('--threads', type=int, default=4,
                       help='Number of threads for parallel processing')
    parser.add_argument('--no-benchmark', action='store_true',
                       help='Skip comprehensive benchmarking')
    
    args = parser.parse_args()
    
    # Determine which files to process
    csv_files = args.files if args.files else args.csv
    
    print("Enhanced Hot Path Detection with Advanced Load Shedding")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = run_enhanced_hotpath_demo(
        csv_files, args.events, not args.no_benchmark, args.threads
    )
    
    print("\nEnhanced hot path demo completed successfully!")
    
    # Generate recommendations
    print(f"\n{'=' * 80}")
    print("RECOMMENDATIONS FOR HOT PATH DETECTION")
    print(f"{'=' * 80}")
    
    print("1. Load Shedding Strategy:")
    print("   - Use semantic strategy for bike-specific priorities")
    print("   - Implement partial match management for bike chains")
    print("   - Apply latency-aware dropping for real-time constraints")
    
    print("\n2. Pattern Optimization:")
    print("   - Focus on rush hour patterns (7-9 AM, 5-7 PM)")
    print("   - Prioritize subscriber trips for better predictability")
    print("   - Weight stations near target locations (7,8,9)")
    
    print("\n3. Performance Tuning:")
    print("   - Monitor bike chain completion rates")
    print("   - Adjust thresholds based on actual usage patterns")
    print("   - Implement predictive load monitoring")
    
    print("\n4. Quality Assurance:")
    print("   - Set minimum recall thresholds (e.g., 85% for 50% latency)")
    print("   - Monitor false positive rates in hot path detection")
    print("   - Implement pattern health scoring")


if __name__ == "__main__":
    main()
