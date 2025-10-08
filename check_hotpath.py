# hot_path_citibike.py
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Iterable, Optional, Set, Tuple, List

"""
Hot path / hot target detection for Citi Bike trips (201810-citibike-tripdata schema).

Expected CSV columns (exact names):
['tripduration', 'starttime', 'stoptime', 'start station id',
 'start station name', 'start station latitude', 'start station longitude',
 'end station id', 'end station name', 'end station latitude',
 'end station longitude', 'bikeid', 'usertype', 'birth year', 'gender']

Core outputs:
  1) Per-window hot source→target paths  : columns [window_start, src, dst, count, rank]
  2) Per-window hot target stations (in) : columns [window_start, dst, count, rank]
  3) Overall hottest targets (optional)  : columns [dst, total_count, windows_touched, rank]
  4) Hottest inbound paths to given dsts : columns [dst, src, total_count, windows_hot, rank]

Default window is fixed 1 hour ("1H"). Chunked streaming avoids loading the whole CSV.
"""

# ---- Internal helpers -------------------------------------------------------


def _normalize_columns_citibike(
    df: pd.DataFrame,
    time_col: str,
    src_col: str,
    dst_col: str,
    parse_dates: bool,
) -> pd.DataFrame:
    # Parse timestamps
    if parse_dates and not np.issubdtype(df[time_col].dtype, np.datetime64):
        # CitiBike old CSVs use local time strings; parse to UTC-naive then set UTC
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce", utc=True)

    # Drop rows missing essentials
    df = df.dropna(subset=[time_col, src_col, dst_col])

    # Station IDs can be floats/strings—make them integers if possible, else strings
    for c in (src_col, dst_col):
        # Strip whitespace then try numeric
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
        try:
            df[c] = pd.to_numeric(df[c], errors="raise").astype("Int64")
        except Exception:
            df[c] = df[c].astype(str)

    return df


def _aggregate_chunk_windowed(
    df: pd.DataFrame,
    time_col: str,
    src_col: str,
    dst_col: str,
    window: str,
    targets: Optional[Set] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - path_counts:  DataFrame [window_start, src, dst, count]
      - target_counts: DataFrame [window_start, dst, count]
    """
    if targets is not None:
        df = df[df[dst_col].isin(targets)]
        if df.empty:
            return (
                pd.DataFrame(columns=["window_start", "src", "dst", "count"]),
                pd.DataFrame(columns=["window_start", "dst", "count"]),
            )

    # Group by fixed windows
    g_path = (
        df.set_index(time_col)
        .groupby([pd.Grouper(freq=window), src_col, dst_col])
        .size()
        .rename("count")
        .reset_index()
        .rename(columns={time_col: "window_start", src_col: "src", dst_col: "dst"})
    )

    g_target = (
        df.set_index(time_col)
        .groupby([pd.Grouper(freq=window), dst_col])
        .size()
        .rename("count")
        .reset_index()
        .rename(columns={time_col: "window_start", dst_col: "dst"})
    )

    return g_path, g_target


# ---- Public API -------------------------------------------------------------


def find_hot_paths_from_csv(
    csv_path: str,
    *,
    time_col: str = "starttime",              # CitiBike column
    src_col: str = "start station id",        # CitiBike column
    dst_col: str = "end station id",          # CitiBike column
    window: str = "1H",
    targets: Optional[Iterable] = None,       # e.g., {7, 8, 9}
    min_count: int = 1,
    top_k_paths_per_window: int = 20,
    top_k_targets_per_window: int = 10,
    parse_dates: bool = True,
    chunksize: int = 200_000,
    usecols: Optional[Iterable[str]] = (
        "starttime", "start station id", "end station id"
    ),
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Stream a CitiBike CSV and compute:
      1) Top source→target paths per window (hot paths)
      2) Top target stations per window (hot targets)

    Returns:
      hot_paths_df  : [window_start, src, dst, count, rank]
      hot_targets_df: [window_start, dst, count, rank]
    """
    targets_set = set(targets) if targets is not None else None
    all_paths: List[pd.DataFrame] = []
    all_targets: List[pd.DataFrame] = []

    read_kwargs = dict(chunksize=chunksize)
    if usecols is not None:
        read_kwargs["usecols"] = list(usecols)

    for chunk in pd.read_csv(csv_path, **read_kwargs):
        # Validate required columns exist in this CSV subset
        missing = {time_col, src_col, dst_col} - set(chunk.columns)
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        chunk = _normalize_columns_citibike(chunk, time_col, src_col, dst_col, parse_dates=parse_dates)
        chunk = chunk[chunk[time_col].notna()]
        if chunk.empty:
            continue

        g_path, g_target = _aggregate_chunk_windowed(
            chunk, time_col, src_col, dst_col, window=window, targets=targets_set
        )
        if not g_path.empty:
            all_paths.append(g_path)
        if not g_target.empty:
            all_targets.append(g_target)

    if not all_paths:
        hot_paths_df = pd.DataFrame(columns=["window_start", "src", "dst", "count", "rank"])
        hot_targets_df = pd.DataFrame(columns=["window_start", "dst", "count", "rank"])
        return hot_paths_df, hot_targets_df

    # Merge partial aggregates across chunks
    paths = pd.concat(all_paths, ignore_index=True)
    targets_df = pd.concat(all_targets, ignore_index=True)

    paths = (
        paths.groupby(["window_start", "src", "dst"], as_index=False)["count"]
        .sum()
    )
    targets_df = (
        targets_df.groupby(["window_start", "dst"], as_index=False)["count"]
        .sum()
    )

    # Filter out low-count cells
    if min_count > 1:
        paths = paths[paths["count"] >= min_count]
        targets_df = targets_df[targets_df["count"] >= min_count]

    # Rank within each window
    paths = paths.sort_values(["window_start", "count"], ascending=[True, False])
    paths["rank"] = (
        paths.groupby("window_start")["count"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    hot_paths_df = paths[paths["rank"] <= int(top_k_paths_per_window)].sort_values(
        ["window_start", "rank", "count"], ascending=[True, True, False]
    )

    targets_df = targets_df.sort_values(["window_start", "count"], ascending=[True, False])
    targets_df["rank"] = (
        targets_df.groupby("window_start")["count"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    hot_targets_df = targets_df[targets_df["rank"] <= int(top_k_targets_per_window)].sort_values(
        ["window_start", "rank", "count"], ascending=[True, True, False]
    )

    return hot_paths_df, hot_targets_df


def find_hottest_targets_overall(
    hot_targets_df: pd.DataFrame,
    top_k_overall: int = 20
) -> pd.DataFrame:
    """
    Aggregate a per-window hot_targets_df to overall hottest targets.

    Returns:
      [dst, total_count, windows_touched, rank]
    """
    if hot_targets_df.empty:
        return pd.DataFrame(columns=["dst", "total_count", "windows_touched", "rank"])

    agg = (
        hot_targets_df.groupby("dst")
        .agg(total_count=("count", "sum"), windows_touched=("window_start", "nunique"))
        .reset_index()
        .sort_values("total_count", ascending=False)
    )
    agg["rank"] = np.arange(1, len(agg) + 1, dtype=int)
    return agg.head(top_k_overall)


def find_hot_paths_to_specific_targets(
    hot_paths_df: pd.DataFrame,
    targets: Iterable,
    top_k_per_target: int = 5
) -> pd.DataFrame:
    """
    From per-window hot paths, select rows leading to 'targets' and
    aggregate sources that are repeatedly hot to those targets.

    Returns:
      [dst, src, total_count, windows_hot, rank]
    """
    tset = set(targets)
    if hot_paths_df.empty:
        return pd.DataFrame(columns=["dst", "src", "total_count", "windows_hot", "rank"])

    df = hot_paths_df[hot_paths_df["dst"].isin(tset)]
    if df.empty:
        return pd.DataFrame(columns=["dst", "src", "total_count", "windows_hot", "rank"])

    agg = (
        df.groupby(["dst", "src"])
        .agg(total_count=("count", "sum"), windows_hot=("window_start", "nunique"))
        .reset_index()
        .sort_values(["dst", "total_count"], ascending=[True, False])
    )
    agg["rank"] = (
        agg.groupby("dst")["total_count"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    return agg[agg["rank"] <= int(top_k_per_target)].sort_values(["dst", "rank"])


# ---- Optional CLI -----------------------------------------------------------

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Find hot target stations and hot source→target paths from CitiBike CSV.")
    p.add_argument("--csv_path", default="201810-citibike-tripdata.csv", help="Path to 201810-citibike-tripdata CSV")
    p.add_argument("--window", default="1H", help="Fixed window size (e.g., 1H, 30T)")
    p.add_argument("--targets", nargs="*", default=None, help="Optional list of target station ids to focus on")
    p.add_argument("--min-count", type=int, default=1, help="Minimum count per (window, group) to keep")
    p.add_argument("--topk-paths", type=int, default=20, help="Top K paths per window")
    p.add_argument("--topk-targets", type=int, default=10, help="Top K targets per window")
    p.add_argument("--chunksize", type=int, default=200_000, help="CSV chunksize for streaming")
    p.add_argument("--save-prefix", default=None, help="If set, save CSV outputs with this prefix")
    args = p.parse_args()

    targets = None
    if args.targets:
        # Try to coerce to int; if fails, keep strings
        t_coerced = []
        for t in args.targets:
            try:
                t_coerced.append(int(t))
            except Exception:
                t_coerced.append(t)
        targets = t_coerced

    hot_paths_df, hot_targets_df = find_hot_paths_from_csv(
        csv_path=args.csv_path,
        time_col="starttime",
        src_col="start station id",
        dst_col="end station id",
        window=args.window,
        targets=targets,
        min_count=args.min_count,
        top_k_paths_per_window=args.topk_paths,
        top_k_targets_per_window=args.topk_targets,
        parse_dates=True,
        chunksize=args.chunksize,
        usecols=("starttime", "start station id", "end station id"),
    )

    print("\nPer-window hot targets (head 10):")
    print(hot_targets_df.head(10))

    print("\nPer-window hot paths (head 10):")
    print(hot_paths_df.head(10))

    # Overall hottest targets (optional)
    overall = find_hottest_targets_overall(hot_targets_df, top_k_overall=20)
    print("\nOverall hottest targets (top 20):")
    print(overall.head(20))

    # Save if requested
    if args.save_prefix:
        hot_paths_df.to_csv(f"{args.save_prefix}_hot_paths.csv", index=False)
        hot_targets_df.to_csv(f"{args.save_prefix}_hot_targets.csv", index=False)
        overall.to_csv(f"{args.save_prefix}_overall_hot_targets.csv", index=False)
        print(f"\nSaved to: {args.save_prefix}_hot_paths.csv, {args.save_prefix}_hot_targets.csv, {args.save_prefix}_overall_hot_targets.csv")
