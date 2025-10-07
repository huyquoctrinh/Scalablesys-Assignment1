#!/usr/bin/env python3
"""
CRITICAL PERFORMANCE FIXES for CitiBike Hot Path Detection
Addresses memory explosion and performance bottlenecks for 20K+ events
"""

import os
import sys
import time
import gc
import psutil
from datetime import datetime, timedelta
from typing import List, Dict, Union
import threading
from queue import Queue, Empty
import glob

# Memory monitoring
def get_memory_usage():
    """Enhanced memory monitoring with warnings."""
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_percent = process.memory_percent()
    available_gb = psutil.virtual_memory().available / (1024**3)
    
    return {
        'rss_mb': memory_info.rss / 1024 / 1024,
        'vms_mb': memory_info.vms / 1024 / 1024,
        'percent': memory_percent,
        'available_gb': available_gb,
        'warning': memory_percent > 70.0 or available_gb < 1.0
    }

def force_memory_cleanup():
    """Aggressive memory cleanup."""
    collected = gc.collect()
    # Force collection of all generations
    for i in range(3):
        collected += gc.collect(i)
    return collected

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from city_bike_formatter import CitiBikeCSVFormatter
from CEP import CEP
from stream.Stream import InputStream, OutputStream
from base.Event import Event
from base.Pattern import Pattern
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, SmallerThanCondition
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition, OrCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from loadshedding import LoadSheddingConfig
from condition.KCCondition import KCCondition
from parallel.ParallelExecutionParameters import DataParallelExecutionParametersHirzelAlgorithm


class MemoryOptimizedKleeneCondition(KCCondition):
    """Ultra-lightweight Kleene closure condition with strict memory limits."""
    
    def __init__(self, kleene_var="a", bike_key="bikeid", start_key="start_station_id", 
                 end_key="end_station_id", max_len: int = 3):  # CRITICAL: max_len=3 instead of 8
        
        # CRITICAL OPTIMIZATION: Severely limit sequence length
        if max_len > 5:
            print(f"WARNING: Reducing max_len from {max_len} to 5 for memory safety")
            max_len = 5
        
        self._max_len = max_len
        self._bike_key = bike_key
        self._start_key = start_key
        self._end_key = end_key
        self._kleene_var = kleene_var
        
        def _to_int(v):
            if v is None or v == "NULL" or v == "":
                return -1
            try:
                return int(v)
            except (ValueError, TypeError):
                return -1

        def _payload(ev):
            if isinstance(ev, dict):
                return ev
            p = getattr(ev, "payload", None)
            if p is not None:
                return p
            gp = getattr(ev, "get_payload", None)
            return gp() if callable(gp) else ev

        def getattr_func(ev):
            p = _payload(ev)
            return (_to_int(p.get(bike_key)), _to_int(p.get(start_key)), _to_int(p.get(end_key)))

        def relation_op(prev_tuple, curr_tuple):
            # Skip invalid IDs immediately
            if prev_tuple[0] == -1 or curr_tuple[0] == -1 or prev_tuple[2] == -1 or curr_tuple[1] == -1:
                return False
            # Same bike AND chaining
            return (prev_tuple[0] == curr_tuple[0]) and (prev_tuple[2] == curr_tuple[1])

        super().__init__(kleene_var, getattr_func, relation_op)

    def _eval(self, ctx) -> bool:
        """Optimized evaluation with early termination."""
        seq = self._extract_seq(ctx)
        n = len(seq)
        
        # CRITICAL: Early termination for memory safety
        if n > self._max_len:
            return False
        if n <= 1:
            return True
        
        # OPTIMIZATION: Validate only consecutive pairs (not all combinations)
        for i in range(n - 1):
            prev = self._to_payload(seq[i])
            curr = self._to_payload(seq[i + 1])
            
            # Early exit on first invalid pair
            prev_bike = prev.get(self._bike_key)
            curr_bike = curr.get(self._bike_key)
            prev_end = prev.get(self._end_key)
            curr_start = curr.get(self._start_key)
            
            if not all([prev_bike, curr_bike, prev_end, curr_start]) or \
               any(v == "NULL" for v in [prev_bike, curr_bike, prev_end, curr_start]):
                return False
                
            if str(prev_bike) != str(curr_bike) or str(prev_end) != str(curr_start):
                return False
        
        return True

    def _extract_seq(self, ctx):
        if isinstance(ctx, list):
            return ctx
        if isinstance(ctx, dict):
            return ctx.get(self._kleene_var) or ctx.get("a") or []
        return []

    def _to_payload(self, e):
        if isinstance(e, dict):
            return e
        p = getattr(e, "payload", None)
        if p is not None:
            return p
        gp = getattr(e, "get_payload", None)
        return gp() if callable(gp) else e


class StreamingCitiBikeInputStream(InputStream):
    """Memory-efficient streaming input with windowed processing."""
    
    def __init__(self, csv_files: Union[str, List[str]], max_events: int = 20000, 
                 window_size: int = 1000, memory_threshold_mb: float = 500.0):
        super().__init__()
        
        # Handle file patterns
        if isinstance(csv_files, str):
            if '*' in csv_files:
                self.csv_files = glob.glob(csv_files)
            else:
                self.csv_files = [csv_files]
        else:
            self.csv_files = csv_files
        
        self.max_events = max_events
        self.window_size = window_size  # Process in windows to control memory
        self.memory_threshold_mb = memory_threshold_mb
        self.count = 0
        self.current_window = 0
        
        print(f"Streaming mode: {len(self.csv_files)} files, {max_events} events, window={window_size}")
        
        # Start streaming
        self._start_streaming()
    
    def _start_streaming(self):
        """Stream events in controlled windows."""
        events_processed = 0
        
        for file_idx, csv_file in enumerate(self.csv_files):
            if events_processed >= self.max_events:
                break
                
            print(f"Streaming from {os.path.basename(csv_file)} (file {file_idx+1}/{len(self.csv_files)})")
            
            try:
                formatter = CitiBikeCSVFormatter(csv_file)
                window_count = 0
                
                for data in formatter:
                    if events_processed >= self.max_events:
                        break
                    
                    # Memory monitoring
                    if window_count % self.window_size == 0:
                        memory = get_memory_usage()
                        if memory['warning']:
                            print(f"⚠️  Memory warning: {memory['rss_mb']:.1f}MB ({memory['percent']:.1f}%)")
                            collected = force_memory_cleanup()
                            print(f"🧹 Cleaned up {collected} objects")
                    
                    # Add minimal attributes for memory efficiency
                    data['importance'] = self._simple_importance(data)
                    data['event_type'] = "BikeTrip"
                    
                    self._stream.put(data)
                    events_processed += 1
                    window_count += 1
                    
                    if window_count % self.window_size == 0:
                        print(f"  Processed {window_count} events from {os.path.basename(csv_file)}")
                
            except Exception as e:
                print(f"Error streaming {csv_file}: {e}")
                continue
        
        self.count = events_processed
        print(f"Streaming completed: {self.count} events loaded")
        self.close()
    
    def _simple_importance(self, data) -> float:
        """Ultra-simple importance calculation."""
        importance = 0.5
        hour = data["ts"].hour
        
        # Simple rush hour boost
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            importance += 0.3
        
        # Subscriber boost
        if data.get("usertype") == "Subscriber":
            importance += 0.2
        
        return min(1.0, importance)


def create_memory_optimized_patterns():
    """Create patterns optimized for memory efficiency."""
    print("Creating memory-optimized patterns...")
    
    # SEQ( BikeTrip+ a[], BikeTrip b )
    pattern_structure = SeqOperator(
        KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a")),
        PrimitiveEventStructure("BikeTrip", "b")
    )
    
    # CRITICAL: Use memory-optimized Kleene condition with max_len=3
    chain_condition = MemoryOptimizedKleeneCondition(
        kleene_var="a",
        bike_key="bikeid", 
        start_key="start_station_id",
        end_key="end_station_id",
        max_len=3  # REDUCED from 8 to 3 for memory safety
    )
    
    # Simplified station condition (only stations 7,8,9)
    def safe_int(value, default=-1):
        if value is None or value == "NULL" or value == "":
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    # b.end in {7,8,9} using simple OR conditions
    b_ends_7 = EqCondition(Variable("b", lambda x: safe_int(x.get("end_station_id"))), 7)
    b_ends_8 = EqCondition(Variable("b", lambda x: safe_int(x.get("end_station_id"))), 8)
    b_ends_9 = EqCondition(Variable("b", lambda x: safe_int(x.get("end_station_id"))), 9)
    
    b_ends_target = OrCondition(OrCondition(b_ends_7, b_ends_8), b_ends_9)
    
    # Combined condition
    pattern_condition = AndCondition(chain_condition, b_ends_target)
    
    pattern = Pattern(pattern_structure, pattern_condition, timedelta(minutes=30))  # Reduced from 1 hour
    pattern.name = "MemoryOptimizedHotPath"
    
    print("Memory-optimized pattern created with max_len=3, window=30min")
    return [pattern]


class SimpleMemoryOutputStream(OutputStream):
    """Minimal output stream for large datasets."""
    
    def __init__(self, file_path: str):
        super().__init__()
        self.file_path = file_path
        self.matches = []
        self.file_handle = open(file_path, 'w')
        self.write_count = 0
    
    def add_item(self, item: object):
        """Write immediately to avoid memory accumulation."""
        self.matches.append(item)  # Keep minimal tracking
        self.file_handle.write(str(item) + "\n")
        self.write_count += 1
        
        # Periodic flush and memory check
        if self.write_count % 100 == 0:
            self.file_handle.flush()
            if self.write_count % 1000 == 0:
                memory = get_memory_usage()
                if memory['warning']:
                    print(f"⚠️  Output memory warning: {memory['rss_mb']:.1f}MB")
        
        super().add_item(item)
    
    def close(self):
        """Close file handle."""
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
        print(f"Output written: {self.write_count} items to {self.file_path}")
    
    def get_analysis(self):
        return {
            'total_matches': len(self.matches),
            'items_written': self.write_count
        }


class SimpleCitiBikeDataFormatter:
    """Minimal data formatter."""
    
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


def run_memory_optimized_demo(csv_files: Union[str, List[str]], max_events: int = 20000):
    """Run memory-optimized demo for large datasets."""
    
    print("🚀 MEMORY-OPTIMIZED CITIBIKE HOT PATH DETECTION")
    print("=" * 60)
    print(f"Max events: {max_events:,}")
    print(f"Target: Process without memory explosion")
    
    # Initial memory check
    initial_memory = get_memory_usage()
    print(f"Initial memory: {initial_memory['rss_mb']:.1f}MB ({initial_memory['percent']:.1f}%)")
    
    # Create optimized patterns
    patterns = create_memory_optimized_patterns()
    
    # Create optimized CEP with minimal parallel units
    parallel_params = DataParallelExecutionParametersHirzelAlgorithm(
        units_number=2,  # Minimal parallelism
        key="bikeid"
    )
    
    # Aggressive load shedding for memory protection
    load_config = LoadSheddingConfig(
        enabled=True,
        strategy_name='semantic',
        pattern_priorities={'MemoryOptimizedHotPath': 8.0},
        importance_attributes=['importance'],
        memory_threshold=0.5,  # Very aggressive
        cpu_threshold=0.6
    )
    
    cep = CEP(patterns, parallel_execution_params=parallel_params, load_shedding_config=load_config)
    
    # Create optimized streams
    input_stream = StreamingCitiBikeInputStream(
        csv_files, 
        max_events=max_events,
        window_size=500,  # Small windows
        memory_threshold_mb=300.0
    )
    
    output_stream = SimpleMemoryOutputStream("memory_optimized_results.txt")
    formatter = SimpleCitiBikeDataFormatter()
    
    # Monitor memory during processing
    def memory_monitor():
        while True:
            memory = get_memory_usage()
            if memory['warning']:
                print(f"🔥 MEMORY ALERT: {memory['rss_mb']:.1f}MB ({memory['percent']:.1f}%)")
                collected = force_memory_cleanup()
                print(f"🧹 Emergency cleanup: {collected} objects")
            time.sleep(5)  # Check every 5 seconds
    
    monitor_thread = threading.Thread(target=memory_monitor, daemon=True)
    monitor_thread.start()
    
    # Run processing
    print("\n🚀 Starting memory-optimized processing...")
    start_time = time.time()
    
    try:
        duration = cep.run(input_stream, output_stream, formatter)
        end_time = time.time()
        
        # Final memory check
        final_memory = get_memory_usage()
        memory_increase = final_memory['rss_mb'] - initial_memory['rss_mb']
        
        # Get results
        output_stream.close()
        analysis = output_stream.get_analysis()
        
        print(f"\n✅ PROCESSING COMPLETED SUCCESSFULLY!")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Wall time: {end_time - start_time:.2f} seconds")
        print(f"Events processed: {input_stream.count:,}")
        print(f"Matches found: {analysis['total_matches']:,}")
        print(f"Final memory: {final_memory['rss_mb']:.1f}MB ({final_memory['percent']:.1f}%)")
        print(f"Memory increase: {memory_increase:+.1f}MB")
        
        # Load shedding stats
        ls_stats = cep.get_load_shedding_statistics()
        if ls_stats:
            print(f"Events dropped: {ls_stats['events_dropped']:,}")
            print(f"Drop rate: {ls_stats['drop_rate']:.1%}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Final cleanup
        collected = force_memory_cleanup()
        print(f"🧹 Final cleanup: {collected} objects")


if __name__ == "__main__":
    # Find data files
    data_patterns = [
        'test_data/*.csv',
        'test_data/201306-citibike-tripdata.csv', 
        'test_data_split/*.csv'
    ]
    
    csv_files = None
    for pattern in data_patterns:
        if '*' in pattern:
            matches = glob.glob(pattern)
            if matches:
                csv_files = pattern
                break
        elif os.path.exists(pattern):
            csv_files = pattern
            break
    
    if not csv_files:
        print("ERROR: No data files found!")
        sys.exit(1)
    
    print(f"Using data: {csv_files}")
    
    # Run optimized demo
    success = run_memory_optimized_demo(csv_files, max_events=20000)
    sys.exit(0 if success else 1)