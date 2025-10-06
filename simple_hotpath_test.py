#!/usr/bin/env python3
"""
Simple Hot Path Test - Minimal version to debug issues
This script tests the hot path detection with minimal complexity.
"""

import os
import sys
import time
from datetime import datetime, timedelta

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
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from loadshedding import LoadSheddingConfig
from condition.KCCondition import KCCondition


class AdjacentChainingKC(KCCondition):
    """Simplified Kleene closure condition for bike chaining."""
    
    def __init__(self, kleene_var="a", bike_key="bikeid", start_key="start_station_id", end_key="end_station_id"):
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
            return (_to_int(p.get(bike_key)), _to_int(p.get(start_key)), _to_int(p.get(end_key)))

        def relation_op(prev_tuple, curr_tuple):
            return (prev_tuple[0] == curr_tuple[0]) and (prev_tuple[2] == curr_tuple[1])

        super().__init__(kleene_var, getattr_func, relation_op)
        self._kleene_var = kleene_var

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
            p = prev if isinstance(prev, dict) else prev.payload if hasattr(prev, 'payload') else {}
            c = curr if isinstance(curr, dict) else curr.payload if hasattr(curr, 'payload') else {}
            if str(p.get("bikeid")) != str(c.get("bikeid")):
                return False
            if str(p.get("end_station_id")) != str(c.get("start_station_id")):
                return False
        return True


class SimpleHotPathStream(InputStream):
    """Simple stream for hot path testing."""
    
    def __init__(self, csv_file: str, max_events: int = 1000):
        super().__init__()
        self.formatter = CitiBikeCSVFormatter(csv_file)
        self.max_events = max_events
        self.count = 0
        
        print(f"Loading data from {csv_file}...")
        for data in self.formatter:
            if self.count >= max_events:
                break
            
            # Add basic attributes
            data['importance'] = 0.5
            data['priority'] = 5.0
            data['event_type'] = "BikeTrip"
            
            self._stream.put(data)
            self.count += 1
            
            if self.count % 100 == 0:
                print(f"Loaded {self.count}/{max_events} events...")
        
        self.close()
        print(f"Finished loading {self.count} events")


class SimpleOutputStream(OutputStream):
    """Simple output stream."""
    
    def __init__(self):
        super().__init__()
        self.matches = []
    
    def add_item(self, item):
        self.matches.append(item)
        super().add_item(item)
        print(f"Match found: {len(self.matches)} total matches")
    
    def get_matches(self):
        return self.matches


class SimpleDataFormatter(DataFormatter):
    """Simple data formatter."""
    
    def __init__(self):
        class SimpleClassifier(EventTypeClassifier):
            def get_event_type(self, event_payload):
                return "BikeTrip"
        
        super().__init__(SimpleClassifier())
    
    def parse_event(self, raw_data):
        return raw_data if isinstance(raw_data, dict) else {"data": str(raw_data)}
    
    def get_event_timestamp(self, event_payload):
        if "ts" in event_payload:
            ts = event_payload["ts"]
            return ts.timestamp() if hasattr(ts, 'timestamp') else float(ts)
        return datetime.now().timestamp()
    
    def format(self, data):
        return str(data)


def create_simple_hot_path_pattern():
    """Create a simple hot path pattern for testing."""
    print("Creating simple hot path pattern...")
    
    # SEQ( BikeTrip+ a[], BikeTrip b )
    pattern_structure = SeqOperator(
        KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a")),
        PrimitiveEventStructure("BikeTrip", "b")
    )
    
    # Chain a[] (adjacent trips by same bike)
    chain_condition = AdjacentChainingKC(
        kleene_var="a",
        bike_key="bikeid",
        start_key="start_station_id",
        end_key="end_station_id"
    )
    
    # Same bike between a[last] and b
    same_bike_condition = EqCondition(
        Variable("a", lambda x: x["bikeid"]),
        Variable("b", lambda x: x["bikeid"])
    )
    
    # b ends in {7,8,9}
    target_station_condition = AndCondition(
        GreaterThanCondition(Variable("b", lambda x: int(x["end_station_id"])), 6),
        SmallerThanCondition(Variable("b", lambda x: int(x["end_station_id"])), 10)
    )
    
    pattern_condition = AndCondition(
        chain_condition,
        same_bike_condition,
        target_station_condition
    )
    
    pattern = Pattern(
        pattern_structure,
        pattern_condition,
        timedelta(hours=1)  # 1 hour window
    )
    pattern.name = "SimpleHotPath"
    
    print("Pattern created successfully")
    return pattern


def run_simple_test(csv_file: str, max_events: int = 1000):
    """Run a simple test of hot path detection."""
    print("=" * 60)
    print("SIMPLE HOT PATH DETECTION TEST")
    print("=" * 60)
    print(f"CSV file: {csv_file}")
    print(f"Max events: {max_events}")
    
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"Error: File not found: {csv_file}")
        return
    
    try:
        # Create pattern
        pattern = create_simple_hot_path_pattern()
        
        # Create CEP instance (no load shedding for simplicity)
        print("Creating CEP instance...")
        cep = CEP([pattern])
        
        # Create streams
        print("Creating input stream...")
        input_stream = SimpleHotPathStream(csv_file, max_events)
        
        print("Creating output stream...")
        output_stream = SimpleOutputStream()
        
        print("Creating data formatter...")
        data_formatter = SimpleDataFormatter()
        
        # Run processing
        print("Starting pattern matching...")
        start_time = time.time()
        
        duration = cep.run(input_stream, output_stream, data_formatter)
        
        end_time = time.time()
        
        # Results
        print("\n" + "=" * 60)
        print("RESULTS")
        print("=" * 60)
        print(f"Processing time: {duration:.2f} seconds")
        print(f"Wall clock time: {end_time - start_time:.2f} seconds")
        print(f"Events processed: {input_stream.count}")
        print(f"Matches found: {len(output_stream.get_matches())}")
        
        if len(output_stream.get_matches()) > 0:
            print("✓ Hot path detection working!")
        else:
            print("⚠ No matches found - this might be normal depending on data")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Simple Hot Path Detection Test')
    parser.add_argument('--csv', default='citybike_dataset/201306-citibike-tripdata.csv',
                       help='Path to CitiBike CSV file')
    parser.add_argument('--events', type=int, default=1000,
                       help='Maximum number of events to process')
    
    args = parser.parse_args()
    
    print("Simple Hot Path Detection Test")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = run_simple_test(args.csv, args.events)
    
    if success:
        print("\n✓ Test completed successfully!")
    else:
        print("\n❌ Test failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()

