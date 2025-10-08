#!/usr/bin/env python3
"""
Memory-Optimized Hot Path Demo with Advanced Load Shedding
This script demonstrates the complete hot path detection system with memory-safe load shedding capabilities.
Includes memory monitoring and optimized Kleene closure conditions to prevent memory exhaustion.
"""

import os
import sys
import time
import logging
import psutil
import threading
from datetime import datetime, timedelta
from typing import List, Iterator, Dict, Union
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import glob
import gc
import csv

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


class MemoryMonitor:
    """Real-time memory monitoring and protection class."""
    
    def __init__(self, memory_limit_percent=90, check_interval=1.0):  # More lenient limit
        self.memory_limit_percent = memory_limit_percent
        self.check_interval = check_interval
        self.monitoring = False
        self.memory_alerts = []
        self.process = None
        self.monitor_thread = None
        self._initialize_process()
        
    def _initialize_process(self):
        """Initialize process handle (needed for pickling support)."""
        try:
            self.process = psutil.Process()
        except Exception:
            self.process = None
    
    def __getstate__(self):
        """Custom pickle state to exclude non-serializable objects."""
        state = self.__dict__.copy()
        # Remove non-serializable objects
        state['process'] = None
        state['monitor_thread'] = None
        state['monitoring'] = False
        return state
    
    def __setstate__(self, state):
        """Custom unpickle state to restore functionality."""
        self.__dict__.update(state)
        self._initialize_process()
        
    def start_monitoring(self):
        """Start memory monitoring in background thread."""
        if not self.process:
            self._initialize_process()
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_memory, daemon=True)
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        """Stop memory monitoring."""
        self.monitoring = False
        
    def _monitor_memory(self):
        """Background memory monitoring loop."""
        while self.monitoring:
            try:
                memory_percent = psutil.virtual_memory().percent
                process_memory_mb = 0
                if self.process:
                    try:
                        process_memory_mb = self.process.memory_info().rss / 1024 / 1024
                    except Exception:
                        process_memory_mb = 0
                
                if memory_percent > self.memory_limit_percent:
                    alert = {
                        'timestamp': datetime.now(),
                        'system_memory_percent': memory_percent,
                        'process_memory_mb': process_memory_mb,
                        'severity': 'HIGH' if memory_percent > 90 else 'MEDIUM'
                    }
                    self.memory_alerts.append(alert)
                    
                    if memory_percent > 95:
                        logger.critical(f"CRITICAL: System memory at {memory_percent:.1f}%! Process using {process_memory_mb:.1f}MB")
                        # Force garbage collection
                        gc.collect()
                    elif memory_percent > self.memory_limit_percent:
                        logger.warning(f"WARNING: System memory at {memory_percent:.1f}%! Process using {process_memory_mb:.1f}MB")
                        
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Memory monitoring error: {e}")
                time.sleep(self.check_interval)
                
    def get_memory_status(self):
        """Get current memory status."""
        process_memory_mb = 0
        if self.process:
            try:
                process_memory_mb = self.process.memory_info().rss / 1024 / 1024
            except Exception:
                process_memory_mb = 0
                
        return {
            'system_memory_percent': psutil.virtual_memory().percent,
            'process_memory_mb': process_memory_mb,
            'memory_available_gb': psutil.virtual_memory().available / 1024 / 1024 / 1024,
            'alert_count': len(self.memory_alerts)
        }
        
    def is_memory_safe(self):
        """Check if memory usage is within safe limits."""
        return psutil.virtual_memory().percent < self.memory_limit_percent


class MemoryOptimizedCitiBikeCSVFormatter:
    """Memory-optimized CitiBike CSV formatter with batch processing."""
    
    def __init__(self, csv_file: str, batch_size: int = 1000):
        self.csv_file = csv_file
        self.batch_size = batch_size
        
        # Multiple datetime formats to try
        self.datetime_formats = [
            '%Y-%m-%d %H:%M:%S.%f',  # 2018+ format with microseconds
            '%Y-%m-%d %H:%M:%S',     # 2013-2017 format
            '%m/%d/%Y %H:%M:%S',     # Alternative format
            '%m/%d/%Y %H:%M',        # Without seconds
        ]
    
    def __iter__(self):
        """Iterate through CSV data with memory-efficient batch processing."""
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                batch = []
                
                for row_num, row in enumerate(csv_reader, 1):
                    try:
                        # Clean and validate required fields
                        cleaned_row = self._clean_row_data(row)
                        
                        if self._is_valid_trip_data(cleaned_row):
                            batch.append(cleaned_row)
                            
                        # Process batch when full
                        if len(batch) >= self.batch_size:
                            for item in batch:
                                yield item
                            batch = []  # Clear batch to free memory
                            gc.collect()  # Force garbage collection
                        
                    except Exception as e:
                        logger.error(f"Error processing row {row_num} in {self.csv_file}: {e}")
                        continue
                
                # Process remaining items in batch
                for item in batch:
                    yield item
                    
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
            except ValueError as e:
                # Only log the problematic ones for debugging
                if "unconverted data remains" in str(e):
                    logger.debug(f"Format {fmt} failed for '{datetime_str}': {e}")
                continue
        
        # If all formats fail, try to handle edge cases
        try:
            # Handle microsecond parsing issues
            if '.' in datetime_str:
                main_part, micro_part = datetime_str.rsplit('.', 1)
                
                # Clean up the microsecond part - remove any non-digit characters
                clean_micro = ''.join(c for c in micro_part if c.isdigit())
                
                # Pad or truncate to 6 digits
                if len(clean_micro) > 6:
                    clean_micro = clean_micro[:6]
                elif len(clean_micro) < 6:
                    clean_micro = clean_micro.ljust(6, '0')
                
                datetime_str = f"{main_part}.{clean_micro}"
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
                
        except ValueError as e:
            logger.debug(f"Edge case parsing failed for '{datetime_str}': {e}")
        
        # Try without microseconds if the above fails
        try:
            if '.' in datetime_str:
                main_part = datetime_str.split('.')[0]
                return datetime.strptime(main_part, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
        
        # Last resort: return a default datetime
        logger.warning(f"Could not parse datetime '{datetime_str}', using current time")
        return datetime.now()
    
    def _clean_row_data(self, row: dict) -> dict:
        """Clean and standardize row data with memory optimization."""
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


class MemoryOptimizedAdjacentChainingKC(KCCondition):
    """Memory-optimized Kleene closure condition with chain length limits and memory protection."""
    
    def __init__(self, kleene_var="a",
                 bike_key="bikeid",
                 start_key="start_station_id",
                 end_key="end_station_id",
                 max_chain_length=10,  # Prevent memory explosion
                 memory_monitor=None):

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
                if v in ['NULL', 'null', '', None, '\\N']:
                    return -1
                return int(v)
            except (ValueError, TypeError):
                return -1

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
        self.max_chain_length = max_chain_length
        self.memory_monitor = memory_monitor
        
        # Statistics for monitoring
        self.chains_processed = 0
        self.chains_truncated = 0
        self.memory_safe_calls = 0

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
        """Memory-safe evaluation with chain length limits."""
        seq = self._extract_seq(ctx)
        self.chains_processed += 1
        
        # Debug logging every 100 chains
        # if self.chains_processed % 100 == 0:
            # logger.info(f"KC: Processed {self.chains_processed} chains, current length: {len(seq)}")
        
        # Memory safety check (safe even if memory_monitor is None after deepcopy)
        memory_critical = False
        try:
            if self.memory_monitor and hasattr(self.memory_monitor, 'is_memory_safe'):
                memory_critical = not self.memory_monitor.is_memory_safe()
                if memory_critical:
                    self.memory_safe_calls += 1
                    logger.warning(f"Memory limit reached, truncating chain evaluation (chain #{self.chains_processed})")
                    # Only reject if memory is extremely critical (>95%)
                    if hasattr(self.memory_monitor, 'get_memory_status'):
                        status = self.memory_monitor.get_memory_status()
                        if status.get('system_memory_percent', 0) > 95:
                            return False
        except Exception:
            # If memory monitor fails, continue without it
            pass
        
        # Chain length protection - be more lenient
        if len(seq) > self.max_chain_length:
            self.chains_truncated += 1
            if self.chains_truncated <= 10:  # Log first few truncations
                logger.info(f"Chain length {len(seq)} exceeds maximum {self.max_chain_length}, truncating")
            # Truncate but don't make it too short
            seq = seq[:self.max_chain_length]
        
        if len(seq) <= 1:
            return True
            
        # Validate chaining with NULL safety
        for i, (prev, curr) in enumerate(zip(seq, seq[1:])):
            p = self._to_payload(prev)
            c = self._to_payload(curr)

            # Safe comparison handling NULL values
            p_bike = p.get(self._bike_key, '')
            c_bike = c.get(self._bike_key, '')
            p_end = p.get(self._end_key, '-1')
            c_start = c.get(self._start_key, '-1')
            
            # Handle NULL values
            if p_bike in ['NULL', 'null', '', None] or c_bike in ['NULL', 'null', '', None]:
                return False
            if p_end in ['NULL', 'null', '', None] or c_start in ['NULL', 'null', '', None]:
                return False
            
            # Check bike and station chaining
            if str(p_bike) != str(c_bike):
                return False
            if str(p_end) != str(c_start):
                return False
                
            # Memory check every 10 iterations for long chains (less frequent)
            try:
                if i % 10 == 9 and self.memory_monitor and hasattr(self.memory_monitor, 'is_memory_safe'):
                    status = self.memory_monitor.get_memory_status()
                    if status.get('system_memory_percent', 0) > 95:  # Only fail if extremely critical
                        logger.warning(f"Critical memory limit reached during chain validation at position {i}")
                        return False
            except Exception:
                # If memory monitor fails, continue without it
                pass
                
        # Log successful chain validation occasionally
        if len(seq) > 3 and self.chains_processed % 50 == 0:
            logger.info(f"KC: Successfully validated chain of length {len(seq)}")
                
        return True
    
    def get_statistics(self):
        """Get processing statistics."""
        return {
            'chains_processed': self.chains_processed,
            'chains_truncated': self.chains_truncated,
            'memory_safe_calls': self.memory_safe_calls,
            'truncation_rate': self.chains_truncated / max(1, self.chains_processed)
        }


class MemoryOptimizedHotPathStream(InputStream):
    """Memory-optimized stream with controlled memory usage."""
    
    def __init__(self, csv_files: Union[str, List[str]], max_events: int = 5000, 
                 num_threads: int = 2, memory_monitor=None, batch_size: int = 500):
        super().__init__()
        
        # Handle both single file and multiple files
        if isinstance(csv_files, str):
            if '*' in csv_files or '?' in csv_files:
                self.csv_files = glob.glob(csv_files)
            else:
                self.csv_files = [csv_files]
        else:
            self.csv_files = csv_files
        
        # Reduced defaults for memory safety
        self.max_events = min(max_events, 10000)  # Cap at 10K events
        self.num_threads = min(num_threads, 2)    # Limit threads to reduce memory
        self.batch_size = batch_size
        self.memory_monitor = memory_monitor
        self.count = 0
        self.loading_complete = False
        
        # Thread-safe queue for processed events
        self.processed_queue = Queue(maxsize=batch_size * 2)  # Limited queue size
        self.loading_lock = threading.Lock()
        
        # Station analysis for better importance calculation
        self.station_popularity = {}
        self.station_zones = {}
        
        # Load data with memory monitoring
        self._load_data_memory_safe()
        
        print(f"Finished loading {self.count} events with hot path attributes from {len(self.csv_files)} files")
    
    def _load_data_memory_safe(self):
        """Load data with memory monitoring and protection."""
        print(f"Loading hot path data from {len(self.csv_files)} files using {self.num_threads} threads...")
        
        # First pass: analyze stations with memory limits
        self._analyze_stations_limited()
        
        # Second pass: load events with memory monitoring
        events_per_file = self.max_events // len(self.csv_files) if len(self.csv_files) > 1 else self.max_events
        
        for file_idx, csv_file in enumerate(self.csv_files):
            if self.memory_monitor and not self.memory_monitor.is_memory_safe():
                logger.warning(f"Memory limit reached, stopping file processing at file {file_idx}")
                break
                
            try:
                file_count = self._process_csv_file_memory_safe(csv_file, file_idx, events_per_file)
                print(f"Processed {file_count} events from {os.path.basename(csv_file)}")
                
                # Force garbage collection after each file
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error processing {csv_file}: {e}")
        
        # Transfer processed events to main stream with memory checking
        transferred = 0
        while not self.processed_queue.empty() and transferred < self.max_events:
            try:
                if self.memory_monitor and not self.memory_monitor.is_memory_safe():
                    logger.warning("Memory limit reached during event transfer")
                    break
                    
                event_data = self.processed_queue.get_nowait()
                self._stream.put(event_data)
                self.count += 1
                transferred += 1
                
                # Periodic memory checks
                if transferred % 100 == 0:
                    gc.collect()
                    
            except Empty:
                break
        
        self.close()
    
    def _process_csv_file_memory_safe(self, csv_file: str, file_idx: int, max_events_per_file: int) -> int:
        """Process a single CSV file with memory safety."""
        file_count = 0
        
        try:
            formatter = MemoryOptimizedCitiBikeCSVFormatter(csv_file, self.batch_size)
            for data in formatter:
                # Memory safety check
                if self.memory_monitor and not self.memory_monitor.is_memory_safe():
                    logger.warning(f"Memory limit reached in file {csv_file} at event {file_count}")
                    break
                    
                with self.loading_lock:
                    if self.count >= self.max_events or file_count >= max_events_per_file:
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
                
                # Add to processed queue with backpressure
                try:
                    self.processed_queue.put(data, timeout=1.0)
                    file_count += 1
                except:
                    logger.warning("Queue full, skipping event to prevent memory buildup")
                    break
                
                if file_count % 200 == 0:
                    print(f"File {file_idx}: Processed {file_count} events from {os.path.basename(csv_file)}")
                    gc.collect()  # Periodic cleanup
                    
        except Exception as e:
            logger.error(f"Error in memory-safe processing {csv_file}: {e}")
        
        return file_count
    
    def _analyze_stations_limited(self):
        """Analyze stations with memory limits."""
        print("Analyzing stations for hot path detection (memory-limited)...")
        
        station_counts = {}
        analysis_sample_limit = 500  # Reduced sample size
        
        for csv_file in self.csv_files[:3]:  # Limit to first 3 files
            try:
                formatter = MemoryOptimizedCitiBikeCSVFormatter(csv_file)
                sample_count = 0
                
                for data in formatter:
                    if sample_count >= analysis_sample_limit:
                        break
                        
                    start_station = data.get("start_station_id")
                    end_station = data.get("end_station_id")
                    
                    if start_station and start_station != '-1':
                        station_counts[start_station] = station_counts.get(start_station, 0) + 1
                    
                    if end_station and end_station != '-1':
                        station_counts[end_station] = station_counts.get(end_station, 0) + 1
                    
                    sample_count += 1
                    
            except Exception as e:
                logger.error(f"Error analyzing stations in {csv_file}: {e}")
        
        # Calculate popularity scores
        max_count = max(station_counts.values()) if station_counts else 1
        self.station_popularity = {
            station: count / max_count 
            for station, count in station_counts.items()
        }
        
        print(f"Analyzed {len(self.station_popularity)} unique stations (memory-limited)")
    
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
        if start_station is not None and start_station not in ['NULL', 'null', '', None, '\\N']:
            try:
                station_id = int(start_station)
                if 6 < station_id < 10:  # Near target stations
                    importance += 0.3
                elif 5 <= station_id <= 11:  # Close to target stations
                    importance += 0.15
            except (ValueError, TypeError):
                pass  # Skip invalid station IDs
        
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
        if start_station is not None and start_station not in ['NULL', 'null', '', None, '\\N']:
            try:
                station_id = int(start_station)
                if 6 < station_id < 10:  # Target stations
                    priority += 3.0
                elif 5 <= station_id <= 11:  # Near target stations
                    priority += 1.5
            except (ValueError, TypeError):
                pass  # Skip invalid station IDs
        
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
        if start_station is not None and start_station not in ['NULL', 'null', '', None, '\\N']:
            try:
                station_id = int(start_station)
                if 6 < station_id < 10:
                    potential += 0.3
            except (ValueError, TypeError):
                pass  # Skip invalid station IDs
        
        return min(1.0, potential)


def create_memory_optimized_hot_path_patterns(memory_monitor=None):
    """Create memory-optimized hot path detection patterns."""
    logger.info("Creating memory-optimized hot path patterns...")
    patterns = []

    # SEQ( BikeTrip+ a[], BikeTrip b )
    pattern1_structure = SeqOperator(
        KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a")),  # a[] with Kleene +
        PrimitiveEventStructure("BikeTrip", "b")                          # b
    )

    # Memory-optimized chain validation for a[] - more lenient settings
    chain_inside_a = MemoryOptimizedAdjacentChainingKC(
        kleene_var="a",
        bike_key="bikeid",
        start_key="start_station_id",
        end_key="end_station_id",
        max_chain_length=20,  # Increased for better pattern detection
        memory_monitor=memory_monitor
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
    
    # b ends in {7,8,9}
    b_ends_in_target = AndCondition(
        GreaterThanCondition(Variable("b", safe_int_station_id), 6),
        SmallerThanCondition(Variable("b", safe_int_station_id), 10)
    )

    pattern1_condition = AndCondition(
        chain_inside_a,
        same_bike_last_a_b,
        b_ends_in_target
    )

    # Balanced time window - not too restrictive
    pattern1 = Pattern(
        pattern1_structure,
        pattern1_condition,
        timedelta(hours=1)  # Balanced: not too restrictive, not too memory-intensive
    )
    pattern1.name = "MemoryOptimizedHotPathDetection"
    patterns.append(pattern1)

    logger.info(f"Created {len(patterns)} memory-optimized patterns")
    return patterns, chain_inside_a  # Return the KC condition for statistics


class HotPathOutputStream(OutputStream):
    """Output stream with hot path specific analysis."""
    
    def __init__(self, file_path: str = "memory_optimized_hotpath_results.txt", is_async: bool = False):
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
        
        # Log pattern matches
        logger.info(f"🎯 HOT PATH MATCH FOUND! Total matches: {len(self.matches)}")
        logger.info(f"   Match details: {str(item)[:200]}...")
        
        if self.__is_async:
            self.__output_file.write(str(item) + "\n")
            self.__output_file.flush()
        else:
            self.__output_buffer.append(item)

        # RETURN (a[1].start, a[i].end, b.end) extraction
        try:
            if hasattr(item, 'events') and item.events and len(item.events) >= 2:
                a_event = item.events[0]
                b_event = item.events[1]
                a_prims = getattr(a_event, 'primitive_events', None)
                if isinstance(a_prims, list) and len(a_prims) > 0 and hasattr(b_event, 'payload'):
                    first_start = a_prims[0].payload.get("start_station_id")
                    last_end = a_prims[-1].payload.get("end_station_id")
                    b_end = b_event.payload.get("end_station_id")
                    result = ("RETURN", first_start, last_end, b_end)
                    if not self.__is_async:
                        self.__output_buffer.append(result)
                    else:
                        self.__output_file.write(str(result) + "\n")
                        self.__output_file.flush()
        except Exception as e:
            logger.debug(f"Error extracting return values: {e}")
        
        # Analyze hot path pattern
        if hasattr(item, 'pattern_name') or (isinstance(item, dict) and 'pattern' in str(item)):
            pattern_name = getattr(item, 'pattern_name', 'MemoryOptimizedHotPathDetection')
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


class HotPathDataFormatter:
    def __init__(self):
        pass
    
    def get_event_type(self, event_payload):
        return "BikeTrip"
    
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


def run_memory_optimized_hotpath_demo(csv_files: Union[str, List[str]], max_events: int = 3000, 
                                    num_threads: int = 2):
    """Run memory-optimized hot path demonstration."""
    
    # Handle file validation
    if isinstance(csv_files, str):
        if '*' in csv_files or '?' in csv_files:
            resolved_files = glob.glob(csv_files)
            if not resolved_files:
                print(f"Error: No files found matching pattern: {csv_files}")
                return
            csv_files = resolved_files[:2]  # Limit to 2 files for memory safety
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
        csv_files = csv_files[:2]  # Limit to 2 files
    
    print("=" * 80)
    print("MEMORY-OPTIMIZED HOT PATH DETECTION WITH LOAD SHEDDING")
    print("=" * 80)
    print(f"Data files: {len(csv_files)} files")
    for i, f in enumerate(csv_files):
        print(f"  {i+1}. {os.path.basename(f)}")
    print(f"Max events: {max_events}")
    print(f"Threads: {num_threads}")
    
    # Initialize memory monitor
    memory_monitor = MemoryMonitor(memory_limit_percent=95)
    memory_monitor.start_monitoring()
    
    try:
        # Create memory-optimized patterns
        patterns, kc_condition = create_memory_optimized_hot_path_patterns(memory_monitor)
        
        # Test configurations with memory-aware settings
        configs = {
            'Memory Optimized - No Load Shedding': LoadSheddingConfig(enabled=False),
            'Memory Optimized - Conservative': LoadSheddingConfig(
                strategy_name='semantic',
                pattern_priorities={'MemoryOptimizedHotPathDetection': 9.0},
                importance_attributes=['importance', 'priority'],
                memory_threshold=0.70,  # Lower threshold
                cpu_threshold=0.75
            )
        }
        
        results = {}
        
        for config_name, config in configs.items():
            print(f"\n{'-' * 60}")
            print(f"Testing: {config_name}")
            print(f"{'-' * 60}")
            
            # Memory status before processing
            initial_memory = memory_monitor.get_memory_status()
            print(f"  Initial memory: {initial_memory['system_memory_percent']:.1f}% system, {initial_memory['process_memory_mb']:.1f}MB process")
            
            # Create CEP instance
            if config.enabled:
                cep = CEP(patterns, load_shedding_config=config)
            else:
                cep = CEP(patterns)
            
            # Create streams with memory optimization
            input_stream = MemoryOptimizedHotPathStream(
                csv_files, max_events, num_threads, memory_monitor, batch_size=200
            )
            output_stream = HotPathOutputStream(f"memory_optimized_{config_name.replace(' ', '_').lower()}.txt")
            data_formatter = HotPathDataFormatter()
            
            # Run processing with memory monitoring
            print("  Processing events with memory-optimized patterns...")
            start_time = time.time()
            
            try:
                duration = cep.run(input_stream, output_stream, data_formatter)
                end_time = time.time()
                
                # Close output stream
                output_stream.close()
                
                # Get results
                analysis = output_stream.get_analysis()
                kc_stats = kc_condition.get_statistics()
                final_memory = memory_monitor.get_memory_status()
                
                print(f"  ✓ Completed in {duration:.2f} seconds")
                print(f"  ✓ Wall clock time: {end_time - start_time:.2f} seconds")
                print(f"  ✓ Events processed: {input_stream.count}")
                print(f"  ✓ Hot path matches found: {analysis['total_matches']}")
                print(f"  ✓ Unique bike chains: {analysis['unique_bike_chains']}")
                print(f"  ✓ Match rate: {analysis['match_rate']:.3f}")
                print(f"  ✓ Final memory: {final_memory['system_memory_percent']:.1f}% system, {final_memory['process_memory_mb']:.1f}MB process")
                print(f"  ✓ Memory alerts: {final_memory['alert_count']}")
                print(f"  ✓ KC chains processed: {kc_stats['chains_processed']}")
                print(f"  ✓ KC chains truncated: {kc_stats['chains_truncated']} ({kc_stats['truncation_rate']:.1%})")
                print(f"  ✓ Memory-safe calls: {kc_stats['memory_safe_calls']}")
                
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
                    'kc_statistics': kc_stats,
                    'memory_initial': initial_memory,
                    'memory_final': final_memory,
                    'load_shedding': cep.get_load_shedding_statistics() if cep.is_load_shedding_enabled() else None
                }
                
            except Exception as e:
                logger.error(f"Error during processing: {e}")
                print(f"  ✗ Processing failed: {e}")
                final_memory = memory_monitor.get_memory_status()
                print(f"  ✓ Memory at failure: {final_memory['system_memory_percent']:.1f}% system")
        
        # Summary comparison
        print(f"\n{'=' * 80}")
        print("MEMORY-OPTIMIZED HOT PATH DETECTION SUMMARY")
        print(f"{'=' * 80}")
        
        for config_name, result in results.items():
            analysis = result['analysis']
            print(f"\n{config_name}:")
            print(f"  Duration: {result['duration']:.2f}s")
            print(f"  Hot Path Matches: {analysis['total_matches']}")
            print(f"  Unique Bike Chains: {analysis['unique_bike_chains']}")
            print(f"  Match Rate: {analysis['match_rate']:.3f}")
            print(f"  Memory Change: {result['memory_initial']['process_memory_mb']:.1f}MB → {result['memory_final']['process_memory_mb']:.1f}MB")
            print(f"  KC Truncation Rate: {result['kc_statistics']['truncation_rate']:.1%}")
            
            if result['load_shedding']:
                ls = result['load_shedding']
                print(f"  Drop Rate: {ls['drop_rate']:.1%}")
                print(f"  Throughput: {ls.get('avg_throughput_eps', 0):.1f} EPS")
        
        return results
        
    finally:
        memory_monitor.stop_monitoring()


def main():
    """Main function for memory-optimized hot path detection."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Memory-Optimized Hot Path Detection')
    parser.add_argument('--csv', default='citybike_dataset/201306-citibike-tripdata.csv',
                       help='Path to CitiBike CSV file(s). Supports glob patterns')
    parser.add_argument('--files', nargs='+',
                       help='Multiple CSV files to process')
    parser.add_argument('--events', type=int, default=3000,
                       help='Maximum number of events to process (reduced default for memory safety)')
    parser.add_argument('--threads', type=int, default=2,
                       help='Number of threads for parallel processing (reduced for memory safety)')
    
    args = parser.parse_args()
    
    # Determine which files to process
    csv_files = args.files if args.files else args.csv
    
    print("Memory-Optimized Hot Path Detection with Advanced Load Shedding")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"System Memory: {psutil.virtual_memory().percent:.1f}% used")
    print(f"Available Memory: {psutil.virtual_memory().available / 1024 / 1024 / 1024:.1f} GB")
    
    results = run_memory_optimized_hotpath_demo(csv_files, args.events, args.threads)
    
    print("\nMemory-optimized hot path demo completed successfully!")
    
    # Generate recommendations
    print(f"\n{'=' * 80}")
    print("MEMORY OPTIMIZATION RECOMMENDATIONS")
    print(f"{'=' * 80}")
    
    print("1. Memory Management:")
    print("   - Maximum chain length limited to 8 events")
    print("   - Batch processing with periodic garbage collection")
    print("   - Real-time memory monitoring and protection")
    print("   - Reduced time windows (30 minutes vs 1 hour)")
    
    print("\n2. Load Shedding Strategy:")
    print("   - Conservative memory thresholds (70% vs 85%)")
    print("   - Priority-based event dropping during memory pressure")
    print("   - Fail-safe pattern rejection when memory critical")
    
    print("\n3. Performance Tuning:")
    print("   - Limited file processing (max 2 files)")
    print("   - Reduced thread count (max 2 threads)")
    print("   - Queue size limits to prevent memory buildup")
    print("   - Regular memory cleanup and monitoring")
    
    print("\n4. Scalability:")
    print("   - Monitor KC truncation rate (<10% recommended)")
    print("   - Adjust max_chain_length based on memory constraints")
    print("   - Implement adaptive batch sizes based on memory usage")


if __name__ == "__main__":
    main()
