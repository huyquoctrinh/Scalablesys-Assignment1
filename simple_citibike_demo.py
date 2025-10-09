#!/usr/bin/env python3
# Simple CitiBike Baseline (No Load Shedding)
# SASE pattern:
#   PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
#   WHERE a[i+1].bike = a[i].bike
#     AND a[i+1].start = a[i].end
#     AND a[last].bike = b.bike
#     AND b.end in {3165,247,358}
#   WITHIN 1h
#   RETURN (a[1].start, a[i].end, b.end)

import os
import sys
import time
import logging
from collections import Counter
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from city_bike_formatter import CitiBikeCSVFormatter
from CEP import CEP
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter, EventTypeClassifier
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from condition.BaseRelationCondition import EqCondition
from condition.CompositeCondition import AndCondition, OrCondition
from condition.Condition import Variable
from condition.KCCondition import KCCondition
from condition.KCCondition import KCIndexCondition
TARGET_END_STATIONS = [3165, 247, 358]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("citibike-baseline")

# ---------- Raw stream (no LS) ----------
class SimpleCitiBikeStream(InputStream):
    def __init__(self, csv_file: str, max_events: int = 10000):
        super().__init__()
        self.csv_file = csv_file
        self.formatter = CitiBikeCSVFormatter(csv_file)
        self.max_events = int(max_events)
        self.count = 0
        self.target_end_count = 0  # end ∈ {3165,247,358}
        self._gen = self._create()

    def _create(self):
        logger.info(f"[STREAM] reading {self.csv_file} (max_events={self.max_events})")
        for row in self.formatter:
            if self.count >= self.max_events:
                break
            row["event_type"] = "BikeTrip"  # keep types simple
            # quick count for sanity
            try:
                end_id = int(row.get("end_station_id")) if row.get("end_station_id") is not None else None
                if end_id in (3165, 247, 358):
                    self.target_end_count += 1
            except Exception:
                pass

            self.count += 1
            if self.count == 1 or (self.count % max(1, self.max_events // 50) == 0):
                logger.info(f"[STREAM] yielded {self.count} events")
            yield row

        logger.info(f"[STREAM] finished: yielded {self.count} events; end∈{{3165,247,358}}≈{self.target_end_count}")

    def get_item(self):
        try:
            return next(self._gen)
        except StopIteration:
            self.close()
            return None

    def __iter__(self): return self
    def __next__(self):
        item = self.get_item()
        if item is None: raise StopIteration
        return item

# ---------- Output collector ----------
class SimpleOutputStream(OutputStream):
    def __init__(self, file_path: str = "output_citybike_baseline.txt"):
        super().__init__()
        self.file_path = file_path
        self._matches = []

    def add_item(self, item: object):
        self._matches.append(item)
        super().add_item(item)

    def get_matches(self): return self._matches

    def close(self):
        super().close()
        with open(self.file_path, "w", encoding="utf-8") as f:
            for m in self._matches:
                f.write(str(m) + "\n")

# ---------- Formatter ----------
class CitiBikeEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload: dict):
        return event_payload.get("event_type") or "BikeTrip"

class SimpleCitiBikeDataFormatter(DataFormatter):
    def __init__(self, total_events_hint: int | None = None, log_every: int | None = None):
        super().__init__(CitiBikeEventTypeClassifier())
        self.total_hint = total_events_hint
        self.log_every = int(log_every) if log_every else (max(1, (self.total_hint or 100000) // 100))
        self._n = 0
        self._t0 = time.time()
        logger.info(f"[FORMATTER] log_every={self.log_every}, total_hint={self.total_hint}")

    def _progress(self):
        self._n += 1
        if self._n == 1 or (self._n % self.log_every) == 0:
            dt = max(1e-9, time.time() - self._t0)
            rate = self._n / dt
            if self.total_hint:
                pct = 100.0 * self._n / self.total_hint
                logger.info(f"[PROGRESS] {self._n}/{self.total_hint} ({pct:.1f}%) — {rate:.0f} ev/s")
            else:
                logger.info(f"[PROGRESS] {self._n} events — {rate:.0f} ev/s")

    def parse_event(self, raw_data):
        self._progress()
        e = dict(raw_data) if isinstance(raw_data, dict) else {"raw": str(raw_data)}

        # normalize ints
        def as_int(k):
            if k in e and e[k] is not None:
                try: e[k] = int(e[k])
                except: pass
        for col in ("bikeid", "start_station_id", "end_station_id", "tripduration_s"):
            as_int(col)

        # normalize timestamp
        ts = e.get("ts")
        if isinstance(ts, str):
            try:
                ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except Exception:
                try:
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except Exception:
                    ts = None
        e["ts"] = ts if ts is not None else datetime.now()
        return e

    def get_event_timestamp(self, event_payload: dict):
        ts = event_payload.get("ts")
        if isinstance(ts, datetime): return ts
        try: return datetime.fromisoformat(str(ts))
        except: return datetime.now()

    def format(self, data): return str(data)

# ---------- KCCondition: adjacency inside a[] ----------
class AdjacentChainingKC(KCCondition):
    # a[i+1].bike == a[i].bike  AND  a[i+1].start == a[i].end
    def __init__(self, kleene_var="a", bike_key="bikeid", start_key="start_station_id", end_key="end_station_id"):
        def getattr_func(ev):
            p = ev if isinstance(ev, dict) else getattr(ev, "payload", ev)
            def to_int(v):
                try: return int(v)
                except: return v
            return (to_int(p.get(bike_key)), to_int(p.get(start_key)), to_int(p.get(end_key)))

        def relation_op(prev_tuple, curr_tuple):
            return (prev_tuple[0] == curr_tuple[0]) and (prev_tuple[2] == curr_tuple[1])

        # IMPORTANT: KCCondition expects a SET of names
        super().__init__({kleene_var}, getattr_func, relation_op)

    def _eval(self, event_list: list = None):
        # True si hay 0/1 eventos (no hay parejas que romper), si no, comprobar todas las adyacencias
        if not event_list or len(event_list) < 2:
            return True
        prev = self._getattr_func(event_list[0])
        for ev in event_list[1:]:
            curr = self._getattr_func(ev)
            if not self._relation_op(prev, curr):
                return False
            prev = curr
        return True

# ---------- Build the exact pattern ----------
def build_pattern():
    structure = SeqOperator(
        KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a")),
        PrimitiveEventStructure("BikeTrip", "b")
    )

    # --- helpers robustos ---
    def _payload(e):
        return e if isinstance(e, dict) else getattr(e, "payload", e)

    def _get_int(p, *keys):
        """Int tolerante: usa el primer key presente; acepta '123', '123.0', etc."""
        for k in keys:
            if k in p and p[k] is not None:
                try:
                    return int(p[k])
                except Exception:
                    try:
                        s = str(p[k]).strip()
                        if s == "" or s.lower() == "nan":
                            continue
                        return int(float(s))
                    except Exception:
                        continue
        return -10**12  # sentinel imposible para no igualar por accidente

    def _a_last_bike(a_binding):
        ev = a_binding[-1] if isinstance(a_binding, (list, tuple)) and a_binding else a_binding
        p = _payload(ev)
        return _get_int(p, "bikeid")

    def _b_bike(b_binding):
        p = _payload(b_binding)
        return _get_int(p, "bikeid")

    def _b_end(b_binding):
        p = _payload(b_binding)
        # soporta ambas variantes de nombre
        return _get_int(p, "end_station_id", "end station id")

    # --- KC: adyacencia dentro de a[]: mismo bike y end == next start ---
    debug_seen = {"n": 0}  # ← contador local para debug

    def _attr(ev):
        p = _payload(ev)
        bike = _get_int(p, "bikeid")
        start = _get_int(p, "start_station_id", "start station id")
        end = _get_int(p, "end_station_id", "end station id")

        # DEBUG (primeras 6 muestras)
        if debug_seen["n"] < 6:
            print(f"[KC attr] bike={bike} start={start} end={end}")
            debug_seen["n"] += 1

        return (bike, start, end)

    def _adj(prev_tuple, curr_tuple):
        return (prev_tuple[0] == curr_tuple[0]) and (prev_tuple[2] == curr_tuple[1])

    chain_inside_a = KCIndexCondition({"a"}, _attr, _adj, offset=1)

    # --- a[last].bike == b.bike ---
    same_bike_last_a_b = EqCondition(
        Variable("a", _a_last_bike),
        Variable("b", _b_bike)
    )

    # --- b.end ∈ TARGET_END_STATIONS ---
    targets = list(TARGET_END_STATIONS)
    conds = [EqCondition(Variable("b", _b_end), v) for v in targets]
    if not conds:
        b_end_is_target = EqCondition(Variable("b", _b_end), -1)  # imposible
    else:
        orc = conds[0]
        for nxt in conds[1:]:
            orc = OrCondition(orc, nxt)
        b_end_is_target = orc

    cond = AndCondition(chain_inside_a, same_bike_last_a_b, b_end_is_target)
    pattern = Pattern(structure, cond, timedelta(hours=1))
    pattern.name = "HotPathDetection"
    return pattern

# ---------- Helpers for nice samples ----------
def extract_tuple_from_match(m):
    """Return (a[1].start, a[last].end, b.end) if possible; else None."""
    def payload(e):
        if e is None: return None
        if isinstance(e, dict): return e
        return getattr(e, "payload", e)

    try:
        a_seq = None; b_ev = None
        if isinstance(m, dict):
            a_seq = m.get("a"); b_ev = m.get("b")
        else:
            try: a_seq, b_ev = m["a"], m["b"]
            except Exception:
                a_seq = getattr(m, "a", None); b_ev = getattr(m, "b", None)
        if not a_seq or b_ev is None: return None
        if not isinstance(a_seq, (list, tuple)): a_seq = [a_seq]
        a1 = payload(a_seq[0]); al = payload(a_seq[-1]); b = payload(b_ev)
        return (int(a1["start_station_id"]), int(al["end_station_id"]), int(b["end_station_id"]))
    except Exception:
        return None

# ---------- Runner ----------
def run(csv_file: str, max_events: int):
    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        print("Please ensure the file exists and the path is correct.")
        return

    logger.info("=" * 72)
    logger.info("CITIBIKE BASELINE — correctness-only (no load shedding)")
    logger.info("=" * 72)
    logger.info(f"CSV: {csv_file}")
    logger.info(f"Max events: {max_events}")

    cep = CEP([build_pattern()])  # NO LS config passed
    input_stream = SimpleCitiBikeStream(csv_file, max_events)
    output_stream = SimpleOutputStream("output_citybike_baseline.txt")
    formatter = SimpleCitiBikeDataFormatter(total_events_hint=max_events, log_every=max(1, max_events // 100))

    t0 = time.time()
    duration = cep.run(input_stream, output_stream, formatter)
    wall = time.time() - t0

    n_events = input_stream.count
    n_matches = len(output_stream.get_matches())
    eps = (n_events / duration) if duration > 0 else 0.0

    print("\n" + "=" * 60)
    print("RUN SUMMARY (baseline)")
    print("=" * 60)
    print(f"CSV file           : {csv_file}")
    print(f"Events processed   : {n_events}")
    print(f"Events end∈{{3165,247,358}}: {input_stream.target_end_count}")
    print(f"Matches found      : {n_matches}")
    print(f"Engine time        : {duration:.2f}s")
    print(f"Wall-clock time    : {wall:.2f}s")
    print(f"Avg throughput     : {eps:.0f} ev/s")
    print("Load shedding      : Disabled")

    # Top-5 (a[1].start, b.end) pairs + a few sample full triples
    pair_counter = Counter()
    sample = []
    for m in output_stream.get_matches():
        tup = extract_tuple_from_match(m)
        if tup is None: continue
        a1_start, _al_end, b_end = tup
        pair_counter[(a1_start, b_end)] += 1
        if len(sample) < 5: sample.append(tup)

    if pair_counter:
        print("\nTop 5 (a[1].start, b.end) pairs:")
        for (s, e), c in pair_counter.most_common(5):
            print(f"  ({s}, {e}) -> {c} matches")

    if sample:
        print("\nSample matches (a[1].start, a[last].end, b.end):")
        for t in sample:
            print(f"  {t}")

    print("\nOutput written to: output_citybike_baseline.txt")


def main():
    import argparse, os
    p = argparse.ArgumentParser(description="CitiBike baseline (no load shedding)")
    # ← valor por defecto para no tener que pasar --csv
    p.add_argument("--csv", default="201810-citibike-tripdata_hot.csv",
                   help="Path to CitiBike CSV file (defaults to 201810-citibike-tripdata_hot.csv)")
    p.add_argument("--events", type=int, default=25,
                   help="Max events to process")
    args = p.parse_args()

    if not os.path.exists(args.csv):
        print(f"Error: CitiBike CSV file not found: {args.csv}")
        print("Place '201810-citibike-tripdata.csv' in the project folder or pass --csv PATH")
        return

    run(args.csv, args.events)
if __name__ == "__main__":
    main()