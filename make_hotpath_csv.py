#!/usr/bin/env python3
import csv
from datetime import datetime, timedelta
from pathlib import Path
import argparse

# Target end stations for b.end (adjust if you want)
TARGET_END_STATIONS = [3165, 247, 358]

# Synthetic BikeIDs (one per target so chains don’t mix)
SYNTH_BIKE_IDS = [900001, 900002, 900003]

# Base time for the 3 matches (all within 1h)
BASE_TIME = datetime(2018, 10, 1, 5, 0, 0)  # 2018-10-01 05:00:00

# Intermediate (fake) stations to build the a[] chains
# a1: S0 -> S1
# a2: S1 -> S2
S0, S1, S2 = 999001, 999002, 999003


def detect_header(csv_path: Path):
    """Read only the header and return the list of columns."""
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    return header


def check_required_columns(header):
    """
    Verify the classic columns exist:
    'starttime','stoptime','start station id','end station id','bikeid'
    """
    required = {"starttime", "stoptime", "start station id", "end station id", "bikeid"}
    missing = [c for c in required if c not in header]
    return missing


def make_row(header, bikeid, start_dt, stop_dt, start_id, end_id):
    """
    Build a dict with the exact columns of the original CSV.
    Fill the essentials; leave the rest blank.
    """
    row = {col: "" for col in header}
    # Essential fields
    row["starttime"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")     # no fraction -> widest compat
    row["stoptime"]  = stop_dt.strftime("%Y-%m-%d %H:%M:%S")
    row["start station id"] = str(int(start_id))
    row["end station id"]   = str(int(end_id))
    row["bikeid"] = str(int(bikeid))
    # If 'tripduration' exists in your CSV, keep it consistent
    for td_name in ("tripduration", "trip duration", "tripduration_s"):
        if td_name in row:
            row[td_name] = str(int((stop_dt - start_dt).total_seconds()))
            break
    # Common fields (if present, leave blank or plausible defaults)
    if "usertype" in row: row["usertype"] = "Subscriber"
    if "gender" in row: row["gender"] = "0"
    if "birth year" in row: row["birth year"] = ""
    # Names and coordinates (if present)
    for nm in ("start station name", "end station name"):
        if nm in row: row[nm] = ""
    for nm in ("start station latitude", "start station longitude",
               "end station latitude", "end station longitude"):
        if nm in row: row[nm] = ""
    return row


def write_hot_csv(in_csv: Path, out_csv: Path, prepend=True):
    header = detect_header(in_csv)
    missing = check_required_columns(header)
    if missing:
        raise RuntimeError(
            "Missing required columns in CSV: "
            + ", ".join(missing)
            + "\nMake sure the file is the classic CitiBike schema (2013–2018)."
        )

    # Prepare 3 guaranteed matches (one per target end station)
    synth_rows = []
    t0 = BASE_TIME

    for idx, (target_end, bikeid) in enumerate(zip(TARGET_END_STATIONS, SYNTH_BIKE_IDS)):
        # a1: S0 -> S1 (same bike)
        a1_start = t0 + timedelta(minutes=idx * 15 + 0)   # spaced 15' per match
        a1_stop  = a1_start + timedelta(minutes=4)
        synth_rows.append(
            make_row(header, bikeid, a1_start, a1_stop, S0, S1)
        )

        # a2: S1 -> S2 (same bike, adjacent to a1: end(a1)=S1 == start(a2)=S1)
        a2_start = a1_start + timedelta(minutes=5)
        a2_stop  = a2_start + timedelta(minutes=4)
        synth_rows.append(
            make_row(header, bikeid, a2_start, a2_stop, S1, S2)
        )

        # b: any start, but same bike and end ∈ TARGET_END_STATIONS
        # Important: b must come after a2 (SEQ) and within 1h from a1.start
        b_start = a2_start + timedelta(minutes=5)
        b_stop  = b_start + timedelta(minutes=6)
        synth_rows.append(
            make_row(header, bikeid, b_start, b_stop, 999004, target_end)
        )

    # Write the new CSV
    with out_csv.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=header)
        writer.writeheader()

        if prepend:
            # First the synthetic rows (so they land in the first N events)
            for r in synth_rows:
                writer.writerow(r)

            # Then copy the original (except its header)
            with in_csv.open("r", encoding="utf-8", newline="") as fin:
                reader = csv.reader(fin)
                next(reader)  # skip original header
                for row in reader:
                    fout.write(",".join(row) + "\n")
        else:
            # Copy the original first, then append synthetic rows at the end
            with in_csv.open("r", encoding="utf-8", newline="") as fin:
                reader = csv.reader(fin)
                next(reader)  # header
                for row in reader:
                    fout.write(",".join(row) + "\n")
            for r in synth_rows:
                writer.writerow(r)

    print(f"[OK] Written: {out_csv}")
    print(f"    Added {len(synth_rows)} synthetic rows (3 guaranteed matches).")
    print(f"    Targets used: {TARGET_END_STATIONS}")


def main():
    ap = argparse.ArgumentParser(description="Generate a CSV with guaranteed hotpaths for OpenCEP.")
    ap.add_argument("--in", dest="in_csv", default="201810-citibike-tripdata.csv",
                    help="Input CSV (CitiBike)")
    ap.add_argument("--out", dest="out_csv", default="201810-citibike-tripdata_hot.csv",
                    help="Output CSV")
    ap.add_argument("--append", action="store_true",
                    help="Append at the end (by default synthetic rows are PREPENDED at the top)")
    args = ap.parse_args()

    in_csv = Path(args.in_csv)
    out_csv = Path(args.out_csv)

    write_hot_csv(in_csv, out_csv, prepend=(not args.append))


if __name__ == "__main__":
    main()
