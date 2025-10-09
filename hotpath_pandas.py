import pandas as pd
from pathlib import Path
from collections import Counter

# Change this if your file lives elsewhere
CSV = Path("201810-citibike-tripdata.csv")
WINDOW = pd.Timedelta("1h")  # a[1].start -> b.end must be within 1 hour

def load_df(csv_path=CSV) -> pd.DataFrame:
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # Expected columns (classic CitiBike schema)
    # 'starttime','stoptime','start station id','end station id','bikeid'
    # Be tolerant with fractional seconds and bad rows
    df["starttime"] = pd.to_datetime(df["starttime"], errors="coerce")
    df["stoptime"] = pd.to_datetime(df["stoptime"], errors="coerce")

    for col in ["start station id", "end station id", "bikeid"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows that break the logic
    df = df.dropna(subset=["starttime", "stoptime", "start station id", "end station id", "bikeid"]).copy()

    # Use integer dtype where possible
    df["start station id"] = df["start station id"].astype("int64")
    df["end station id"]   = df["end station id"].astype("int64")
    df["bikeid"]           = df["bikeid"].astype("int64")

    return df

def find_hotpaths(df: pd.DataFrame, window: pd.Timedelta = WINDOW):
    df = df.sort_values(["bikeid", "starttime"]).reset_index(drop=True)

    pair_counter = Counter()     # (a1.start, b.end) -> count
    station_counter = Counter()  # b.end -> count

    for bike, g in df.groupby("bikeid", sort=False):
        g = g.reset_index(drop=True)
        n = len(g)
        i = 0
        while i < n - 1:
            start_i = i
            # adyacencia a[i+1].start == a[i].end
            while i < n - 1 and (g["end station id"].iat[i] == g["start station id"].iat[i+1]):
                i += 1
            chain_len = i - start_i + 1
            if chain_len >= 2:
                t0 = g["starttime"].iat[start_i]   # scalar Timestamp
                j  = i + 1
                # b puede ser cualquier viaje posterior mientras esté dentro de la 1h
                while j < n and (g["stoptime"].iat[j] - t0) <= window:
                    a1_start = int(g["start station id"].iat[start_i])
                    b_end    = int(g["end station id"].iat[j])
                    pair_counter[(a1_start, b_end)] += 1
                    station_counter[b_end]          += 1
                    j += 1
            # progreso garantizado
            i += 1

    return pair_counter, station_counter

def main():
    df = load_df(CSV)
    print(f"Loaded {len(df):,} rows from {CSV}")

    pair_ctr, station_ctr = find_hotpaths(df, WINDOW)

    # Show top 10 end stations across ALL stations
    print("\nTop 10 b.end stations for hotpaths (not limited to 7/8/9):")
    for st, c in station_ctr.most_common(10):
        print(f"  end_station={st:>6} -> {c} matches")

    # Optionally: top 10 (a1.start, b.end) pairs
    print("\nTop 10 (a[1].start, b.end) pairs:")
    for (s, e), c in pair_ctr.most_common(10):
        print(f"  ({s} -> {e}) -> {c}")

    # count simple adjacency pairs per bike
    pair_count = 0
    for _, g in df.sort_values(["bikeid", "starttime"]).groupby("bikeid"):
        pair_count += (g["end station id"].shift(0).eq(g["start station id"].shift(-1))).sum()
    print("Adjacency pairs:", int(pair_count))

    # If you want to export full counts:
    # pd.Series(station_ctr).rename('count').to_csv("hotpath_top_end_stations.csv")


if __name__ == "__main__":
    main()
