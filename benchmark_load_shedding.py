#!/usr/bin/env python3
"""
Hot Path Detection (OpenCEP, no PredicateCondition) + Load Shedding + Benchmark
- Patterns: Kleene a+ plus fixed 1/2/3-hop chains
- Complexity reductions: b-first target filter (via OR-chain), MAX_LEN guard via SmallerThanCondition,
  light dedup + optional top-K inside KC
- Defensive parsing & debug logging
- Load shedding presets: off | semantic | adaptive
- Benchmark harness: EPS, wall/engine time, match latency p50/p90/p99
"""

import os
import sys
import csv
import glob
import argparse
import time
import statistics
from datetime import datetime, timedelta
from typing import List, Union, Optional

# --- Ensure project modules resolve ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# --- OpenCEP imports (match your codebase names) ---
from CEP import CEP
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter, EventTypeClassifier
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from condition.CompositeCondition import AndCondition, OrCondition
from condition.BaseRelationCondition import EqCondition, SmallerThanCondition
from condition.Condition import Variable
from condition.KCCondition import KCCondition

# --- Load shedding (optional; mirrors your earlier code) ---
LOAD_SHEDDING_AVAILABLE = True
try:
    from loadshedding import (
        LoadSheddingConfig,
        PresetConfigs,                  # optional in some repos; not strictly required
        HotPathLoadSheddingStrategy,    # optional class; not required to import for strategy_name
        LatencyAwareHotPathLoadShedding # optional class; not required to import for strategy_name
    )
except Exception:
    LOAD_SHEDDING_AVAILABLE = False

# ---- Target stations (default full set) ----
DEFAULT_TARGETS = {
    519, 497, 402, 359, 435, 445, 3255, 490, 477, 514,
    491, 426, 520, 3443, 281, 2006, 459, 368, 492, 523,
    3165, 247, 358, 3163
}

# =========================
# CLI flags (set in main)
# =========================
FLAGS = {
    "debug": False,
    "allow_any_b": False,     # if True, ignore TARGETS (sanity mode)
    "kleene_only": False,     # run only the Kleene pattern
    "max_len": 8,             # Kleene MAX length
    "topk_per_bucket": 0,     # 0 disables; e.g., 3 keeps top-3 partials per (bike,last_end)
    "ls_mode": "off",         # off | semantic | adaptive
    "ls_latency_ms": 50,      # for adaptive
}

def dbg(*a):
    if FLAGS["debug"]:
        print("[DEBUG]", *a, flush=True)

# =========================
# CSV formatter (robust)
# =========================
def _to_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(str(x).strip()))
        except Exception:
            return None

class CitiBikeCSVFormatter:
    """Parses a CitiBike CSV row into a normalized dict with ints for IDs."""
    def __init__(self, csv_file: str):
        self.csv_file = csv_file
        self.datetime_formats = [
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M',
            '%m/%d/%Y  %I:%M:%S %p',
            '%m/%d/%Y %I:%M:%S %p',
            '%m/%d/%Y  %I:%M %p',
            '%m/%d/%Y %I:%M %p',
        ]

    def __iter__(self):
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            nin, nout = 0, 0
            for row in reader:
                nin += 1
                cleaned = self._clean(row)
                if cleaned:
                    nout += 1
                    yield cleaned
            dbg(f"Formatter {os.path.basename(self.csv_file)} rows_in={nin} rows_out={nout}")

    def _clean(self, row: dict) -> Optional[dict]:
        def get_any(keys, default=None):
            for k in keys:
                if k in row and str(row[k]).strip():
                    return str(row[k]).strip().strip('"')
            return default

        payload = {}
        payload['starttime']        = get_any(['starttime', 'start_time', 'Start Time'])
        payload['stoptime']         = get_any(['stoptime', 'stop_time', 'Stop Time'])
        payload['tripduration']     = get_any(['tripduration', 'trip_duration', 'Trip Duration'], '0')
        payload['start_station_id'] = get_any(['start_station_id', 'start station id', 'Start Station ID'])
        payload['end_station_id']   = get_any(['end_station_id', 'end station id', 'End Station ID'])
        payload['start_station_name']= get_any(['start_station_name', 'start station name', 'Start Station Name'], '')
        payload['end_station_name'] = get_any(['end_station_name', 'end station name', 'End Station Name'], '')
        payload['bikeid']           = get_any(['bikeid', 'bike_id', 'Bike ID'])
        payload['usertype']         = get_any(['usertype', 'user_type', 'User Type'], 'unknown')

        # convert ids to ints early
        bike_int = _to_int(payload['bikeid']) if payload['bikeid'] else None
        start_int = _to_int(payload['start_station_id']) if payload['start_station_id'] else None
        end_int = _to_int(payload['end_station_id']) if payload['end_station_id'] else None

        if bike_int is None:
            return None
        if start_int is None and end_int is None:
            return None

        payload['bikeid'] = bike_int
        payload['start_station_id'] = start_int
        payload['end_station_id'] = end_int

        payload['ts'] = self._parse_dt(payload['starttime']) or datetime.now()
        payload['tripduration_s'] = _to_int(payload['tripduration']) or 0
        return payload

    def _parse_dt(self, s: str | None) -> Optional[datetime]:
        if not s:
            return None
        s = ' '.join(s.strip().split())
        for fmt in self.datetime_formats:
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                pass
        try:
            if '.' in s:
                return datetime.strptime(s.split('.')[0], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
        return None

# =========================
# Importance signals for load shedding (lightweight)
# =========================
def enrich_importance_fields(p: dict):
    """
    Attach attributes used by semantic/adaptive load shedding:
      - importance, priority (floats)
      - time_criticality (str)
      - station_importance (float baseline)
      - bike_chain_potential (float)
    """
    ts = p.get("ts") or datetime.now()
    hour = ts.hour if hasattr(ts, "hour") else datetime.now().hour
    weekday = ts.weekday() if hasattr(ts, "weekday") else 0

    importance = 0.5
    priority = 5.0

    if weekday < 5:
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            importance += 0.35
            priority += 2.0
        elif 6 <= hour <= 10 or 16 <= hour <= 20:
            importance += 0.2
            priority += 1.0
    else:
        if 10 <= hour <= 18:
            importance += 0.15

    dur = p.get("tripduration_s", 0) or 0
    if dur > 3600: importance += 0.2
    elif dur > 1800: importance += 0.1
    if 600 <= dur <= 3600: priority += 0.5

    station_importance = 0.5
    if p.get("start_station_id") in DEFAULT_TARGETS:
        station_importance += 0.2
        importance += 0.1

    if (p.get("usertype") or "").lower().startswith("sub"):
        importance += 0.1
        priority += 1.0

    bike_chain_potential = 0.5
    if 7 <= hour <= 9 or 17 <= hour <= 19:
        bike_chain_potential += 0.3
    if (p.get("usertype") or "").lower().startswith("sub"):
        bike_chain_potential += 0.2

    p["importance"] = min(1.0, importance)
    p["priority"] = min(10.0, priority)
    p["time_criticality"] = ("rush_hour" if (7 <= hour <= 9 or 17 <= hour <= 19)
                             else "business_hours" if 6 <= hour <= 20
                             else "off_peak")
    p["station_importance"] = station_importance
    p["bike_chain_potential"] = min(1.0, bike_chain_potential)

# =========================
# Input / Output / Formatter
# =========================
class CitiBikeInputStream(InputStream):
    def __init__(self, csv_files: List[str], max_events: int = 100000):
        super().__init__()
        count = 0
        for path in csv_files:
            for payload in CitiBikeCSVFormatter(path):
                payload['event_type'] = 'BikeTrip'
                enrich_importance_fields(payload)  # for semantic/adaptive LS
                self._stream.put(payload)
                count += 1
                if count >= max_events:
                    break
            if count >= max_events:
                break
        dbg("InputStream events_put:", count)
        self.count = count
        self.close()

class HotPathOutputStream(OutputStream):
    def __init__(self, out_path: str = 'hotpath_results.txt'):
        super().__init__()
        self.out_path = out_path
        self._buf = []
        self.matches = 0
        self.match_lat_ms = []  # collect per-match latency in ms

    def add_item(self, item: object):
        self.matches += 1
        ret = None
        latency_ms = None

        try:
            if hasattr(item, 'events') and item.events and len(item.events) >= 2:
                a_ev = item.events[0]
                b_ev = item.events[-1]
                a_prims = getattr(a_ev, 'primitive_events', None)
                # compute latency from b_ev.ts -> now
                ts_b = None
                if hasattr(b_ev, 'payload'):
                    ts_b = b_ev.payload.get('ts')
                if ts_b is not None:
                    now = datetime.now()
                    try:
                        t_b = ts_b if hasattr(ts_b, 'timestamp') else datetime.fromtimestamp(float(ts_b))
                        latency_ms = max(0.0, (now - t_b).total_seconds() * 1000.0)
                    except Exception:
                        pass

                if isinstance(a_prims, list) and len(a_prims) > 0 and hasattr(b_ev, 'payload'):
                    a1_start = a_prims[0].payload.get('start_station_id')
                    ai_end   = a_prims[-1].payload.get('end_station_id')
                    b_end    = b_ev.payload.get('end_station_id')
                    ret = ('RETURN', a1_start, ai_end, b_end)
        except Exception as e:
            dbg("Output extraction error:", e)

        if latency_ms is not None:
            self.match_lat_ms.append(latency_ms)

        self._buf.append(str(item))
        if ret:
            self._buf.append(str(ret))
        super().add_item(item)

    def close(self):
        if self._buf and self.out_path:
            with open(self.out_path, 'w', encoding='utf-8') as f:
                for line in self._buf:
                    f.write(line + '\n')
        self._buf = []
        super().close()

class CitiBikeEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload):
        return "BikeTrip"

class HotPathDataFormatter(DataFormatter):
    def __init__(self):
        super().__init__(event_type_classifier=CitiBikeEventTypeClassifier())

    def get_event_type(self, event_payload):
        return "BikeTrip"
    def get_probability(self, event_payload):
        return None
    def parse_event(self, raw_data):
        return raw_data if isinstance(raw_data, dict) else {"data": str(raw_data)}
    def get_event_timestamp(self, event_payload):
        ts = event_payload.get("ts")
        return ts.timestamp() if hasattr(ts, "timestamp") else float(datetime.now().timestamp())
    def format(self, data):
        return str(data)

# =========================
# Helpers (Variables / getters)
# =========================
def bike_id(ev):   return ev.get('bikeid')
def start_id(ev):  return ev.get('start_station_id')
def end_id(ev):    return ev.get('end_station_id')

# OR-chain membership for b.end in TARGETS (no PredicateCondition available)
def in_targets_or_chain(var):
    eqs = [EqCondition(var, s) for s in _ACTIVE_TARGETS]
    cond = eqs[0]
    for c in eqs[1:]:
        cond = OrCondition(cond, c)
    return cond

# Kleene length as SmallerThanCondition: len(a) <= MAX_LEN
def kleene_len_condition(max_len: int):
    # SmallerThanCondition is strict (<), so compare with max_len+1
    return SmallerThanCondition(Variable('a', lambda seq: len(seq) if seq is not None else 0), max_len + 1)

# =========================
# KCCondition for a+ adjacency with light dedup/top-K
# =========================
class AdjacentChainingKC(KCCondition):
    """
    Inside Kleene a[]:
      - same bike
      - continuous: a[i].end == a[i+1].start
    Also:
      - lightweight dedup on (bike, last_end, length, 5-min bucket) if ts available
      - optional per-(bike,last_end) top-K by length (FLAGS["topk_per_bucket"])
    """
    def __init__(self, kleene_var='a',
                 bike_key='bikeid',
                 start_key='start_station_id',
                 end_key='end_station_id'):
        self._kleene_var = kleene_var
        self._bike_key = bike_key
        self._start_key = start_key
        self._end_key = end_key

        self._seen = set()         # (bike, last_end, length, bucket)
        self._buckets = {}         # (bike,last_end) -> list of lengths (desc)

        def _payload(ev):
            if isinstance(ev, dict): return ev
            p = getattr(ev, 'payload', None)
            if p is not None: return p
            gp = getattr(ev, 'get_payload', None)
            return gp() if callable(gp) else ev

        def _tuple(ev):
            p = _payload(ev)
            return (p.get(bike_key),
                    p.get(start_key),
                    p.get(end_key),
                    p.get("ts"))

        def _rel(prev_t, curr_t):
            return (prev_t[0] == curr_t[0]) and (prev_t[2] == curr_t[1])

        super().__init__({kleene_var}, _tuple, _rel)

    def _bucket_key(self, bike, last_end, length, ts):
        if ts is None:
            return None
        try:
            tsec = ts.timestamp() if hasattr(ts, "timestamp") else float(ts)
            bucket = int(tsec // (5 * 60))
            return (bike, last_end, length, bucket)
        except Exception:
            return None

    def _eval(self, event_list=None):
        if not event_list or len(event_list) < 2:
            return True

        prev = self._getattr_func(event_list[0])
        for ev in event_list[1:]:
            curr = self._getattr_func(ev)
            if not self._relation_op(prev, curr):
                return False
            prev = curr

        # dedup / top-K
        last = self._getattr_func(event_list[-1])
        bike, _, last_end, ts = last
        length = len(event_list)

        key = self._bucket_key(bike, last_end, length, ts)
        if key is not None:
            if key in self._seen:
                return False
            self._seen.add(key)

        topk = FLAGS["topk_per_bucket"]
        if topk > 0:
            bkey = (bike, last_end)
            arr = self._buckets.get(bkey)
            if arr is None:
                arr = []
                self._buckets[bkey] = arr
            arr.append(length)
            arr.sort(reverse=True)
            if len(arr) > topk:
                if length < arr[topk - 1]:
                    return False
                del arr[topk:]

        return True

# =========================
# Patterns
# =========================
_ACTIVE_TARGETS = set(DEFAULT_TARGETS)

def create_hot_path_patterns():
    within_1h = timedelta(hours=1)

    # --- Kleene: SEQ(BikeTrip+ a[], BikeTrip b) ---
    a_plus = KleeneClosureOperator(PrimitiveEventStructure('BikeTrip', 'a'))
    b      = PrimitiveEventStructure('BikeTrip', 'b')

    kc_adj = AdjacentChainingKC('a', 'bikeid', 'start_station_id', 'end_station_id')

    # Variables (avoid repeated lambda allocation)
    a_last_bike = Variable('a', lambda seq: seq and seq[-1].get('bikeid'))
    b_bike      = Variable('b', bike_id)
    b_end       = Variable('b', end_id)

    # WHERE order: 1) b.end in TARGETS (unless allow_any_b)
    #              2) len(a) <= MAX_LEN
    #              3) adjacency inside a[]
    #              4) a[last].bike == b.bike
    conds = []
    if not FLAGS["allow_any_b"]:
        conds.append(in_targets_or_chain(b_end))
    conds.append(kleene_len_condition(FLAGS["max_len"]))
    conds.append(kc_adj)
    conds.append(EqCondition(a_last_bike, b_bike))
    kleene_where = conds[0]
    for c in conds[1:]:
        kleene_where = AndCondition(kleene_where, c)

    hot_kleene = Pattern(SeqOperator(a_plus, b), kleene_where, within_1h)

    # --- Fixed 1 hop: SEQ(a, b) ---
    a1 = PrimitiveEventStructure('BikeTrip', 'a')
    b1 = PrimitiveEventStructure('BikeTrip', 'b')
    conds1 = []
    if not FLAGS["allow_any_b"]:
        conds1.append(in_targets_or_chain(Variable('b', end_id)))
    conds1.extend([
        EqCondition(Variable('a', bike_id), Variable('b', bike_id)),
        EqCondition(Variable('a', end_id),  Variable('b', start_id)),
    ])
    where1 = conds1[0]
    for c in conds1[1:]:
        where1 = AndCondition(where1, c)
    hot1 = Pattern(SeqOperator(a1, b1), where1, within_1h)

    # --- Fixed 2 hops: SEQ(a, c, b) ---
    a2 = PrimitiveEventStructure('BikeTrip', 'a')
    c2 = PrimitiveEventStructure('BikeTrip', 'c')
    b2 = PrimitiveEventStructure('BikeTrip', 'b')
    conds2 = []
    if not FLAGS["allow_any_b"]:
        conds2.append(in_targets_or_chain(Variable('b', end_id)))
    conds2.extend([
        EqCondition(Variable('a', bike_id), Variable('c', bike_id)),
        EqCondition(Variable('c', bike_id), Variable('b', bike_id)),
        EqCondition(Variable('a', end_id),  Variable('c', start_id)),
        EqCondition(Variable('c', end_id),  Variable('b', start_id)),
    ])
    where2 = conds2[0]
    for c in conds2[1:]:
        where2 = AndCondition(where2, c)
    hot2 = Pattern(SeqOperator(a2, c2, b2), where2, within_1h)

    # --- Fixed 3 hops: SEQ(a, c, d, b) ---
    a3 = PrimitiveEventStructure('BikeTrip', 'a')
    c3 = PrimitiveEventStructure('BikeTrip', 'c')
    d3 = PrimitiveEventStructure('BikeTrip', 'd')
    b3 = PrimitiveEventStructure('BikeTrip', 'b')
    conds3 = []
    if not FLAGS["allow_any_b"]:
        conds3.append(in_targets_or_chain(Variable('b', end_id)))
    conds3.extend([
        EqCondition(Variable('a', bike_id), Variable('c', bike_id)),
        EqCondition(Variable('c', bike_id), Variable('d', bike_id)),
        EqCondition(Variable('d', bike_id), Variable('b', bike_id)),
        EqCondition(Variable('a', end_id),  Variable('c', start_id)),
        EqCondition(Variable('c', end_id),  Variable('d', start_id)),
        EqCondition(Variable('d', end_id),  Variable('b', start_id)),
    ])
    where3 = conds3[0]
    for c in conds3[1:]:
        where3 = AndCondition(where3, c)
    hot3 = Pattern(SeqOperator(a3, c3, d3, b3), where3, within_1h)

    if FLAGS["kleene_only"]:
        return [hot_kleene]
    return [hot_kleene, hot1, hot2, hot3]

# =========================
# Benchmark helpers
# =========================
def percentile(vals, p):
    if not vals: return None
    vals = sorted(vals)
    k = (len(vals)-1)*p
    f = int(k)
    c = min(f+1, len(vals)-1)
    if f == c: return vals[f]
    return vals[f] + (vals[c]-vals[f])*(k-f)

def run_once(csv_files, events, out_path, ls_mode, ls_latency_ms, kleene_only=False):
    """Run CEP once, return metrics dictionary."""
    patterns = create_hot_path_patterns()
    ls_config = build_load_shedding_config(ls_mode, ls_latency_ms)
    cep = CEP(patterns, load_shedding_config=ls_config) if (ls_config and getattr(ls_config, "enabled", False)) else CEP(patterns)

    input_stream = CitiBikeInputStream(csv_files, max_events=events)
    output_stream = HotPathOutputStream(out_path)
    formatter = HotPathDataFormatter()

    t0 = time.perf_counter()
    duration = cep.run(input_stream, output_stream, formatter)  # engine-reported duration
    t1 = time.perf_counter()
    wall = t1 - t0

    output_stream.close()

    processed = getattr(input_stream, "count", events)
    eps = processed / wall if wall > 0 else float('inf')

    lats = output_stream.match_lat_ms
    p50 = percentile(lats, 0.30)
    p90 = percentile(lats, 0.50)
    p99 = percentile(lats, 0.90)

    ls_stats = None
    if hasattr(cep, "is_load_shedding_enabled") and cep.is_load_shedding_enabled():
        if hasattr(cep, "get_load_shedding_statistics"):
            ls_stats = cep.get_load_shedding_statistics()

    return {
        "events": processed,
        "matches": output_stream.matches,
        "wall_s": wall,
        "engine_duration_s": duration,
        "eps": eps,
        "lat_p50_ms": p50,
        "lat_p90_ms": p90,
        "lat_p99_ms": p99,
        "ls_stats": ls_stats,
    }

# =========================
# Load shedding config builder
# =========================
def build_load_shedding_config(ls_mode: str, latency_ms: int):
    """
    - off: disabled
    - semantic: strategy_name='semantic' (uses importance/priorities)
    - adaptive: strategy_name='adaptive' (latency-aware)
    """
    if not LOAD_SHEDDING_AVAILABLE:
        return None

    if ls_mode == "off":
        return LoadSheddingConfig(enabled=False)

    if ls_mode == "semantic":
        return LoadSheddingConfig(
            strategy_name='semantic',
            pattern_priorities={
                'RushHourHotPath': 10.0,
                'SubscriberHotPath': 8.0,
                'PopularStationHotPath': 7.0,
                'LongDistanceHotPath': 6.0,
                'RegularHotPath': 5.0
            },
            importance_attributes=['importance', 'priority', 'time_criticality',
                                   'station_importance', 'bike_chain_potential'],
            memory_threshold=0.75,
            cpu_threshold=0.85
        )

    if ls_mode == "adaptive":
        return LoadSheddingConfig(
            strategy_name='adaptive',
            memory_threshold=0.7,
            cpu_threshold=0.8,
            latency_threshold_ms=max(1, int(latency_ms))
        )

    return LoadSheddingConfig(enabled=False)

# =========================
# Runner
# =========================
def resolve_csv_files(path_or_glob: Union[str, List[str]]) -> List[str]:
    if isinstance(path_or_glob, list):
        return path_or_glob
    if any(ch in path_or_glob for ch in ['*', '?', '[']):
        files = glob.glob(path_or_glob)
        if not files:
            raise FileNotFoundError(f'No files matched: {path_or_glob}')
        return files
    if not os.path.exists(path_or_glob):
        raise FileNotFoundError(path_or_glob)
    return [path_or_glob]

def main():
    global _ACTIVE_TARGETS
    parser = argparse.ArgumentParser(description='Hot Path Detection (OpenCEP, no PredicateCondition) + Load Shedding + Benchmark')
    parser.add_argument('--csv', type=str, required=True,
                        help='Path to a CSV or a glob pattern (e.g. data/*.csv)')
    parser.add_argument('--events', type=int, default=50000,
                        help='Max events to process')
    parser.add_argument('--out', type=str, default='hotpath_results.txt',
                        help='Output results file')
    parser.add_argument('--debug', action='store_true', help='Enable debug prints')
    parser.add_argument('--kleene-only', action='store_true', help='Evaluate only the Kleene pattern')
    parser.add_argument('--max-len', type=int, default=8, help='MAX length for Kleene a[]')
    parser.add_argument('--topk', type=int, default=0, help='Top-K partials per (bike,last_end); 0 disables')
    parser.add_argument('--allow-any-b', action='store_true',
                        help='Ignore TARGETS (bypass end-station filter) for sanity checking')
    parser.add_argument('--targets', type=str, default='',
                        help='Comma-separated target station IDs (overrides default set)')
    # load shedding controls
    parser.add_argument('--ls', type=str, default='off', choices=['off', 'semantic', 'adaptive'],
                        help='Load shedding mode')
    parser.add_argument('--ls-latency-ms', type=int, default=50,
                        help='Latency target (ms) for adaptive LS')
    # benchmark controls
    parser.add_argument('--bench', action='store_true', help='Run benchmark (multiple iterations)')
    parser.add_argument('--bench-iters', type=int, default=3, help='Benchmark iterations')
    parser.add_argument('--warmup', type=int, default=1, help='Warmup runs (not recorded)')

    args = parser.parse_args()

    FLAGS["debug"] = args.debug
    FLAGS["kleene_only"] = args.kleene_only
    FLAGS["max_len"] = max(1, args.max_len)
    FLAGS["topk_per_bucket"] = max(0, args.topk)
    FLAGS["ls_mode"] = args.ls
    FLAGS["ls_latency_ms"] = args.ls_latency_ms

    if args.targets:
        _ACTIVE_TARGETS = set(_to_int(x) for x in args.targets.split(',') if x.strip())
    else:
        _ACTIVE_TARGETS = set(DEFAULT_TARGETS)

    if FLAGS["allow_any_b"]:
        dbg("SANITY MODE: allow b at any end station (ignoring TARGETS)")

    if FLAGS["ls_mode"] != "off" and not LOAD_SHEDDING_AVAILABLE:
        print("[HotPath] WARNING: loadshedding package not available; running without LS.")

    csv_files = resolve_csv_files(args.csv)
    print(f'[HotPath] Files: {len(csv_files)} | Max events: {args.events}')
    dbg("Targets:", sorted([t for t in _ACTIVE_TARGETS if t is not None])[:20], "...")

    # ------- Single run (no benchmark) -------
    if not args.bench:
        patterns = create_hot_path_patterns()
        ls_config = build_load_shedding_config(FLAGS["ls_mode"], FLAGS["ls_latency_ms"])
        ls_on = bool(ls_config and getattr(ls_config, "enabled", False))
        cep = CEP(patterns, load_shedding_config=ls_config) if ls_on else CEP(patterns)

        print(f'[HotPath] Load Shedding: {FLAGS["ls_mode"] if ls_on else "off"}'
              f'{(" (" + str(FLAGS["ls_latency_ms"]) + " ms)") if FLAGS["ls_mode"]=="adaptive" and ls_on else ""}')

        input_stream = CitiBikeInputStream(csv_files, max_events=args.events)
        output_stream = HotPathOutputStream(args.out)
        formatter = HotPathDataFormatter()

        print('[HotPath] Running CEP...')
        t0 = time.perf_counter()
        duration = cep.run(input_stream, output_stream, formatter)
        t1 = time.perf_counter()
        output_stream.close()

        wall = t1 - t0
        events = getattr(input_stream, "count", args.events)
        eps = events / wall if wall > 0 else float('inf')

        # latency percentiles
        lats = output_stream.match_lat_ms
        p50 = percentile(lats, 0.50)
        p90 = percentile(lats, 0.90)
        p99 = percentile(lats, 0.99)

        print(f'[HotPath] Done in {duration:.2f}s (engine) | {wall:.2f}s (wall). EPS ~ {eps:.1f}')
        if lats:
            print(f'[HotPath] Matches: {output_stream.matches} | Latency ms (p50/p90/p99): '
                  f'{p50:.1f}/{p90:.1f}/{p99:.1f}')
        else:
            print('[HotPath] Matches: 0 (no latency stats)')

        if hasattr(cep, "is_load_shedding_enabled") and cep.is_load_shedding_enabled():
            if hasattr(cep, "get_load_shedding_statistics"):
                stats = cep.get_load_shedding_statistics()
                if stats:
                    print("[HotPath] LS Stats:",
                          f"strategy={stats.get('strategy')},",
                          f"events_dropped={stats.get('events_dropped')},",
                          f"drop_rate={stats.get('drop_rate')},",
                          f"avg_throughput_eps={stats.get('avg_throughput_eps')}")
        return

    # ------- Benchmark mode -------
    print(f"[HotPath] Benchmark mode: warmup={args.warmup}, iters={args.bench_iters}, ls={FLAGS['ls_mode']}")
    # warmup
    for i in range(args.warmup):
        _ = run_once(csv_files, args.events, args.out, FLAGS["ls_mode"], FLAGS["ls_latency_ms"], FLAGS["kleene_only"])

    # measure
    results = []
    for i in range(args.bench_iters):
        r = run_once(csv_files, args.events, args.out, FLAGS["ls_mode"], FLAGS["ls_latency_ms"], FLAGS["kleene_only"])
        results.append(r)
        print(f"[Bench] iter {i+1}: eps={r['eps']:.1f}, matches={r['matches']}, wall={r['wall_s']:.2f}s, "
              f"lat(ms) p50/p90/p99={r['lat_p50_ms'] and round(r['lat_p50_ms'],1)}/"
              f"{r['lat_p90_ms'] and round(r['lat_p90_ms'],1)}/{r['lat_p99_ms'] and round(r['lat_p99_ms'],1)}")

    # aggregate
    eps_vals = [r['eps'] for r in results]
    match_vals = [r['matches'] for r in results]
    wall_vals = [r['wall_s'] for r in results]
    p50s = [r['lat_p50_ms'] for r in results if r['lat_p50_ms'] is not None]
    p90s = [r['lat_p90_ms'] for r in results if r['lat_p90_ms'] is not None]
    p99s = [r['lat_p99_ms'] for r in results if r['lat_p99_ms'] is not None]

    def avg(x): return sum(x)/len(x) if x else None

    print("\n[Bench] Summary:")
    print(f"  EPS avg={avg(eps_vals):.1f}  min={min(eps_vals):.1f}  max={max(eps_vals):.1f}")
    print(f"  Wall (s) avg={avg(wall_vals):.2f}")
    print(f"  Matches avg={avg(match_vals):.1f}")
    if p50s:
        print(f"  Latency(ms) p50 avg={avg(p50s):.1f}  p90 avg={avg(p90s):.1f}  p99 avg={avg(p99s):.1f}")

if __name__ == '__main__':
    main()
