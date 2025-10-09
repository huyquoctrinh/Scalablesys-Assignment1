# csv_bike_formatter.py
from datetime import datetime
import csv

class CitiBikeCSVFormatter:
    """
    Each row -> a dict (primitive event):
      type: "trip"
      ts:   event timestamp (use starttime)
      plus parsed fields (ints/floats where sensible)
    """
    def __init__(self, filepath, tzinfo=None):
        self.filepath = filepath
        self.tzinfo = tzinfo

    def __iter__(self):
        with open(self.filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Parse/clean fields
                def as_int(x):
                    try: return int(x)
                    except: return None
                def as_float(x):
                    try: return float(x)
                    except: return None

                event = {
                    "type": "trip",
                    "tripduration_s": as_int(row["tripduration"].strip('"')),
                    "starttime": row["starttime"].strip('"'),
                    "stoptime": row["stoptime"].strip('"'),
                    "start_station_id": row["start station id"].strip('"'),
                    "start_station_name": row["start station name"].strip('"'),
                    "start_lat": as_float(row["start station latitude"].strip('"')),
                    "start_lng": as_float(row["start station longitude"].strip('"')),
                    "end_station_id": row["end station id"].strip('"'),
                    "end_station_name": row["end station name"].strip('"'),
                    "end_lat": as_float(row["end station latitude"].strip('"')),
                    "end_lng": as_float(row["end station longitude"].strip('"')),
                    "bikeid": row["bikeid"].strip('"'),
                    "usertype": row["usertype"].strip('"'),
                    "birth_year": (None if row["birth year"] in {"\\N", "", None} else row["birth year"].strip('"')),
                    "gender": row["gender"].strip('"'),
                }

                # Timestamp for CEP: use trip starttime
                s = str(event["starttime"]).strip('"').strip()
                # intenta con microsegundos y, si no, sin microsegundos
                try:
                    ts = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    ts = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

                if self.tzinfo:
                    ts = ts.replace(tzinfo=self.tzinfo)
                event["ts"] = ts

                yield event

if __name__ == "__main__":
    # Example usage
    formatter = CitiBikeCSVFormatter("201306-citibike-tripdata.csv")
    for event in formatter:
        print(event)

