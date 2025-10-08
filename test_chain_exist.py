import pandas as pd
from glob import glob
import json 
def write_json(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=2)
def as_int(s):
    try:
        return int(s)
    except:
        try: return int(float(str(s).strip()))
        except: return None
if __name__ == "__main__":
    res = {}
    list_of_df = glob("citybike_dataset/*.csv")
    print("Found", len(list_of_df), "CSV files")
    for df_path in list_of_df:
        df = pd.read_csv(df_path, usecols=[
            "starttime","stoptime","start station id","end station id","bikeid"
        ]).dropna()
        df["start_id"] = df["start station id"].map(as_int)
        df["end_id"]   = df["end station id"].map(as_int)
        df["bikeid"]   = df["bikeid"].map(as_int)
        df = df.dropna(subset=["start_id","end_id","bikeid"])

        # TARGETS = {3165, 247, 358, 3163}
        TARGETS = {
            519, 497, 402, 359, 435, 445, 3255, 490, 477, 514,
            491, 426, 520, 3443, 281, 2006, 459, 368, 492, 523, 3165, 247, 358, 3163
        }

        # sort by time to emulate Seq order
        df["starttime"] = pd.to_datetime(df["starttime"], errors="coerce")
        df["stoptime"]  = pd.to_datetime(df["stoptime"], errors="coerce")
        df = df.dropna(subset=["starttime","stoptime"]).sort_values("starttime")

        # build a mapping from (bikeid, start_id) -> rows starting there
        from collections import defaultdict
        starts = defaultdict(list)
        for i, r in df.iterrows():
            starts[(r["bikeid"], r["start_id"])].append(i)

        # Count 1-hop chains a->b on same bike with station continuity and hot end
        one_hop = 0
        for i, a in df.iterrows():
            # candidate next trips must be same bike & start at a.end_id
            for j in starts.get((a["bikeid"], a["end_id"]), []):
                b = df.loc[j]
                if b["starttime"] <= a["stoptime"]:
                    continue  # ensure order
                if (b["end_id"] in TARGETS) and ((b["starttime"] - a["starttime"]).total_seconds() <= 3600):
                    one_hop += 1

        print("Potential 1-hop chains in 1k sample in file csv:", one_hop, "in", df_path)
        res[df_path] = one_hop
    write_json(res, "chain_exist.json")
