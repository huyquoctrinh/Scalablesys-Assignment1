#!/usr/bin/env python3
"""
Simple CitiBike Load Shedding Demo
This script demonstrates load shedding with real CitiBike data in a straightforward way.
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Iterator
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, SmallerThanCondition
from condition.Condition import Variable
from stream.FileStream import FileInputStream, FileOutputStream
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
from condition.BaseRelationCondition import BaseRelationCondition
from condition.CompositeCondition import CompositeCondition
from condition.KCCondition import KCCondition
from condition.CompositeCondition import CompositeCondition
from condition.CompositeCondition import AndCondition
from base.Event import Event
from base.Pattern import Pattern
from stream.Stream import InputStream, OutputStream
import time
import os
from datetime import datetime, timedelta
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from loadshedding import LoadSheddingConfig, PresetConfigs
LOAD_SHEDDING_AVAILABLE = True


class SimpleCitiBikeStream(InputStream):
    """Simple stream wrapper for CitiBike data."""

    def __init__(self, csv_file: str, max_events: int = 10000):
        super().__init__()
        self.csv_file = csv_file
        self.formatter = CitiBikeCSVFormatter(csv_file)
        self.max_events = max_events
        self.count = 0
        self._generator = self._create_generator()

        # Load data into the internal queue like FileInputStream does
        #print(f"Loading CitiBike data from {csv_file}...")
        # for data in self.formatter:
        #    if self.count >= max_events:
        #        break
                
            # Add load shedding attributes directly to the data
        #    data['importance'] = self._get_importance(data)
        #    data['priority'] = self._get_priority(data)
        #    data['event_type'] = "BikeTrip"
            
            # Put the data into the internal queue
        #    self._stream.put(data)
        #    self.count += 1
            
        #    if self.count % 100 == 0:  # Log every 100th event during loading
        #        print(f"Loaded {self.count}/{max_events} events...")
        
        # Close the stream to signal end of data
        #self.close()
        #print(f"Finished loading {self.count} events into stream")
    
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

    def _create_generator(self):
        logger.info(f"Starting to stream CitiBike data from {self.csv_file}...")
        for data in self.formatter:
            if self.count >= self.max_events:
                break

            # enrich
            data['importance'] = self._get_importance(data)
            data['priority']   = self._get_priority(data)
            data['event_type'] = "BikeTrip"

            self.count += 1

            # (opcional) mini log para ver que fluye el stream
            if self.count == 1 or (self.count % 1000) == 0:
                logger.info(f"[STREAM] yielded {self.count} events")

            yield data

        logger.info(f"Finished streaming {self.count} events.")

    def get_item(self):
        """El motor CEP llamará a este método para obtener el siguiente evento."""
        try:
            return next(self._generator)
        except StopIteration:
            # Se acabaron los datos
            self.close()
            return None

    def __iter__(self):
        return self

    def __next__(self):
        item = self.get_item()
        if item is None:
            raise StopIteration
        return item


class SimpleOutputStream(OutputStream):
    """Simple output stream to collect results."""
    
    def __init__(self, file_path: str = "output_citybike.txt"):
        super().__init__()  # Initialize the parent Stream class
        self.file_path = file_path
        self.matches = []
    
    def add_item(self, item: object):
        """Add item to both internal list and parent stream."""
        self.matches.append(item)
        # Also add to the parent stream's queue
        super().add_item(item)
    
    def get_matches(self):
        return self.matches
    
    def close(self):
        """Write all matches to file when closing."""
        super().close()
        with open(self.file_path, 'w') as f:
            for match in self.matches:
                f.write(str(match) + '\n')


class CitiBikeEventTypeClassifier(EventTypeClassifier):
    """Event type classifier for CitiBike events."""
    
    def get_event_type(self, event_payload: dict):
        """All CitiBike events are trip events."""
        return "BikeTrip"


class SimpleCitiBikeDataFormatter(DataFormatter):
    """Simple data formatter for CitiBike events."""
    def __init__(self, total_events_hint: int | None = None, log_every: int = 1000):
        super().__init__(CitiBikeEventTypeClassifier())
        self.total_hint = total_events_hint
        self.log_every = max(1, log_every)
        self._processed = 0
        self._t0 = time.time()
        logger.info(f"[PROGRESS-SETUP] log_every={self.log_every}, total_hint={self.total_hint}")

    def _tick_progress(self):
        self._processed += 1
        if self._processed == 1 or (self._processed % self.log_every) == 0:
            elapsed = max(1e-9, time.time() - self._t0)
            rate = self._processed / elapsed
            if self.total_hint:
                pct = 100.0 * self._processed / self.total_hint
                eta = (self.total_hint - self._processed) / rate if rate > 0 else float("inf")
                logger.info(f"[PROGRESS] {self._processed}/{self.total_hint} ({pct:.1f}%) — {rate:.0f} ev/s — ETA {eta:.1f}s")
            else:
                logger.info(f"[PROGRESS] {self._processed} events — {rate:.0f} ev/s")

    def set_temp_data(self, data):
        """Temporary method to store dict data."""
        self._temp_data = data
    
    def parse_event(self, raw_data):
        self._tick_progress()

        if isinstance(raw_data, dict):
            e = dict(raw_data)
            for col in ("bikeid", "start_station_id", "end_station_id", "tripduration_s"):
                if col in e:
                    try: e[col] = int(e[col])
                    except Exception: pass
            return e
        if isinstance(raw_data, str):
            import ast
            if raw_data.startswith("{'") or raw_data.startswith('{\"'):
                return ast.literal_eval(raw_data)
            else:
                return {"raw_data": raw_data}
        return {"data": str(raw_data)}

    def get_event_timestamp(self, event_payload: dict):
        ts = event_payload.get("ts")
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts.replace('Z', '+00:00'))
            except Exception:
                try:
                    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
        if isinstance(ts, (int, float)):
            try:
                return datetime.fromtimestamp(ts)
            except Exception:
                pass
        return datetime.now()

    def format(self, data):
        return str(data)

class AdjacentChainingKC(KCCondition):
    def __init__(self, kleene_var="a",
                 bike_key="bikeid",
                 start_key="start_station_id",
                 end_key="end_station_id", max_len: int | None = 8):
        self._max_len = max_len

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
        n = len(seq)
        if self._max_len and n > self._max_len:
            return False
        if n <= 1:
            return True
        prev = self._to_payload(seq[-2]); curr = self._to_payload(seq[-1])
        return (str(prev.get(self._bike_key)) == str(curr.get(self._bike_key))
                and str(prev.get(self._end_key)) == str(curr.get(self._start_key)))

def create_sample_patterns():
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
        end_key="end_station_id",
        max_len=8
    )

    # same bike between a[last] y b
    same_bike_last_a_b = EqCondition(
        Variable("a", lambda x: x["bikeid"]),
        Variable("b", lambda x: x["bikeid"])
    )

    #b finish in {7,8,9}
    b_ends_in_target = AndCondition(
        GreaterThanCondition(Variable("b", lambda x: x["end_station_id"]), 6),
        SmallerThanCondition(Variable("b", lambda x: x["end_station_id"]), 10)
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
            'Semantic (CitiBike-tuned)': LoadSheddingConfig(
                strategy_name='semantic',
                pattern_priorities={'HotPathDetection': 10.0},
                importance_attributes=['importance', 'priority'],
                memory_threshold=0.6,  # más agresivo
                cpu_threshold=0.7
            )
        }
    else:
        configs = {'No Load Shedding': None}
    
    results = {}
    
    for config_name, config in configs.items():
        print(f"\nTesting: {config_name}")
        print("-" * 40)
        
        logger.info(f"Creating CEP instance for {config_name}")
        # Create CEP instance
        if LOAD_SHEDDING_AVAILABLE and config:
            logger.info(f"Using load shedding config: {config}")
            cep = CEP(patterns, load_shedding_config=config)
        else:
            logger.info("Creating CEP without load shedding")
            cep = CEP(patterns)
        
        logger.info("Creating streams and data formatter")
        # Create streams
        input_stream = SimpleCitiBikeStream(csv_file, max_events)
        output_stream = SimpleOutputStream(f"output_citybike_{config_name.replace(' ', '_').lower()}.txt")
        data_formatter = SimpleCitiBikeDataFormatter(
            total_events_hint=max_events,  # hint para % y ETA
            log_every=max(1, max_events // 10)
        )
        
        # Run processing
        print("  Processing events...")
        logger.info("Starting CEP processing...")
        start_time = time.time()
        
        logger.info("Calling cep.run()...")
        duration = cep.run(input_stream, output_stream, data_formatter)
        end_time = time.time()
        logger.info(f"CEP processing completed in {duration:.2f} seconds")
        
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
    parser.add_argument('--events', type=int, default=1000,
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
    
    results = run_basic_citibike_test(args.csv, args.events)
    print("\nDemo completed successfully!", results)

if __name__ == "__main__":
    main()