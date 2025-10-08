#!/usr/bin/env python3
import csv
from datetime import datetime, timedelta
from pathlib import Path
import argparse

# Estaciones destino para b.end (ajústalas si quieres)
TARGET_END_STATIONS = [3165, 247, 358]

# BikeIDs sintéticos (uno por target para que no se mezclen las cadenas)
SYNTH_BIKE_IDS = [900001, 900002, 900003]

# Tiempos base para los 3 matches (todos dentro de 1h)
BASE_TIME = datetime(2018, 10, 1, 5, 0, 0)  # 2018-10-01 05:00:00

# Estaciones intermedias (ficticias) para construir las cadenas a[]
# a1: S0 -> S1
# a2: S1 -> S2
S0, S1, S2 = 999001, 999002, 999003


def detect_header(csv_path: Path):
    """Lee solo el encabezado y devuelve la lista de columnas."""
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    return header


def check_required_columns(header):
    """
    Verifica que estén las columnas clásicas:
    'starttime','stoptime','start station id','end station id','bikeid'
    """
    required = {"starttime", "stoptime", "start station id", "end station id", "bikeid"}
    missing = [c for c in required if c not in header]
    return missing


def make_row(header, bikeid, start_dt, stop_dt, start_id, end_id):
    """
    Crea un dict con las columnas exactas del CSV original.
    Rellena lo imprescindible y deja el resto vacío.
    """
    row = {col: "" for col in header}
    # Campos imprescindibles
    row["starttime"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")     # sin fracción -> compat total
    row["stoptime"]  = stop_dt.strftime("%Y-%m-%d %H:%M:%S")
    row["start station id"] = str(int(start_id))
    row["end station id"]   = str(int(end_id))
    row["bikeid"] = str(int(bikeid))
    # Si existe 'tripduration' en tu CSV, intenta coherencia
    for td_name in ("tripduration", "trip duration", "tripduration_s"):
        if td_name in row:
            row[td_name] = str(int((stop_dt - start_dt).total_seconds()))
            break
    # Campos habituales (si existen, los dejamos vacíos o valores plausibles)
    if "usertype" in row: row["usertype"] = "Subscriber"
    if "gender" in row: row["gender"] = "0"
    if "birth year" in row: row["birth year"] = ""
    # Nombres y coordenadas (si existen)
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
            "Faltan columnas necesarias en el CSV: "
            + ", ".join(missing)
            + "\nRevisa que el archivo sea el CitiBike clásico (2013–2018)."
        )

    # Preparamos 3 matches garantizados (uno por estación destino)
    synth_rows = []
    t0 = BASE_TIME

    for idx, (target_end, bikeid) in enumerate(zip(TARGET_END_STATIONS, SYNTH_BIKE_IDS)):
        # a1: S0 -> S1 (mismo bike)
        a1_start = t0 + timedelta(minutes=idx * 15 + 0)   # separados 15' por match
        a1_stop  = a1_start + timedelta(minutes=4)
        synth_rows.append(
            make_row(header, bikeid, a1_start, a1_stop, S0, S1)
        )

        # a2: S1 -> S2 (mismo bike y adyacente a a1: end(a1)=S1 == start(a2)=S1)
        a2_start = a1_start + timedelta(minutes=5)
        a2_stop  = a2_start + timedelta(minutes=4)
        synth_rows.append(
            make_row(header, bikeid, a2_start, a2_stop, S1, S2)
        )

        # b: cualquier start, pero mismo bike y end ∈ TARGET_END_STATIONS
        # Importante: b debe venir después de a2 (SEQ) y dentro de 1h desde a1.start
        b_start = a2_start + timedelta(minutes=5)
        b_stop  = b_start + timedelta(minutes=6)
        synth_rows.append(
            make_row(header, bikeid, b_start, b_stop, 999004, target_end)
        )

    # Escribimos el nuevo CSV
    with out_csv.open("w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=header)
        writer.writeheader()

        if prepend:
            # Primero los sintéticos (para que entren en los primeros N eventos)
            for r in synth_rows:
                writer.writerow(r)

            # Luego copiamos TODO el original (excepto su header)
            with in_csv.open("r", encoding="utf-8", newline="") as fin:
                reader = csv.reader(fin)
                next(reader)  # saltar header original
                for row in reader:
                    fout.write(",".join(row) + "\n")
        else:
            # Copiamos primero el original y luego añadimos al final
            with in_csv.open("r", encoding="utf-8", newline="") as fin:
                reader = csv.reader(fin)
                next(reader)  # header
                for row in reader:
                    fout.write(",".join(row) + "\n")
            for r in synth_rows:
                writer.writerow(r)

    print(f"[OK] Escrito: {out_csv}")
    print(f"    Se añadieron {len(synth_rows)} filas sintéticas (3 matches garantizados).")
    print(f"    Destinos usados: {TARGET_END_STATIONS}")


def main():
    ap = argparse.ArgumentParser(description="Genera un CSV con hotpaths garantizados para OpenCEP.")
    ap.add_argument("--in", dest="in_csv", default="201810-citibike-tripdata.csv",
                    help="CSV de entrada (CitiBike)")
    ap.add_argument("--out", dest="out_csv", default="201810-citibike-tripdata_hot.csv",
                    help="CSV de salida")
    ap.add_argument("--append", action="store_true",
                    help="Añadir al final (por defecto se PREPENDEN al principio)")
    args = ap.parse_args()

    in_csv = Path(args.in_csv)
    out_csv = Path(args.out_csv)

    write_hot_csv(in_csv, out_csv, prepend=(not args.append))


if __name__ == "__main__":
    main()
