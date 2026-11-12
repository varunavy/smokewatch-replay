"""Build the county-hour feature table for the LSH experiment.

Joins the three replay input files by (county, event_hour). Rows are sorted by
event_hour, then county, so each hour contributes five consecutive rows.
Missing FRP values mean no fire detection, so they are filled with 0. Missing
wind direction is filled with 0 for both sine and cosine (no direction signal).
All other missing values are filled with the column median. Every feature is
then standardized. Standardization uses statistics from the whole table, which is
acceptable for an offline evaluation and is listed as a limitation.
"""
import json
import math
import os

import numpy as np

INPUT_DIR = os.environ.get("INPUT_DIR", "data/replay_input")

FIRMS_FIELDS = ["fire_detection_count", "total_frp_mw", "mean_frp_mw",
                "max_frp_mw", "high_confidence_count", "night_detection_count"]
EPA_FIELDS = ["pm25_ug_m3"]
NOAA_FIELDS = ["temperature_c", "relative_humidity_percent", "wind_speed_m_s",
               "maximum_wind_speed_m_s"]
WIND_DIR = "wind_direction_degrees"
FEATURE_NAMES = (FIRMS_FIELDS + EPA_FIELDS + NOAA_FIELDS
                 + ["wind_dir_sin", "wind_dir_cos"])
SOURCES = (("firms", FIRMS_FIELDS),
           ("epa_pm25", EPA_FIELDS),
           ("noaa_weather", NOAA_FIELDS + [WIND_DIR]))


def read_jsonl(path):
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if isinstance(rec.get("payload"), dict):
                merged = dict(rec)
                merged.update(rec["payload"])
                rec = merged
            yield rec


def num(value):
    if value is None:
        return float("nan")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def load_table(input_dir=INPUT_DIR):
    by_key = {}
    for name, fields in SOURCES:
        for rec in read_jsonl(os.path.join(input_dir, name + ".jsonl")):
            key = (rec["event_hour"], rec["county"])
            row = by_key.setdefault(key, {"_sources": set()})
            if name in row["_sources"]:
                raise ValueError("duplicate %s row for %s" % (name, key))
            row["_sources"].add(name)
            for f in fields:
                row[f] = num(rec.get(f))
    for key, row in by_key.items():
        if len(row["_sources"]) != len(SOURCES):
            raise ValueError("missing source for %s" % (key,))
    keys = sorted(by_key)
    raw = np.empty((len(keys), len(FEATURE_NAMES)))
    for i, key in enumerate(keys):
        row = by_key[key]
        vals = [row[f] for f in FIRMS_FIELDS + EPA_FIELDS + NOAA_FIELDS]
        d = row[WIND_DIR]
        if math.isnan(d):
            vals += [float("nan"), float("nan")]
        else:
            vals += [math.sin(math.radians(d)), math.cos(math.radians(d))]
        raw[i] = vals
    hours = [k[0] for k in keys]
    counties = [k[1] for k in keys]
    return hours, counties, raw


def standardize(raw):
    x = raw.copy()
    missing = np.isnan(x)
    for name in ("mean_frp_mw", "max_frp_mw", "wind_dir_sin", "wind_dir_cos"):
        j = FEATURE_NAMES.index(name)
        x[np.isnan(x[:, j]), j] = 0.0
    med = np.nanmedian(x, axis=0)
    med = np.where(np.isnan(med), 0.0, med)
    remaining = np.isnan(x)
    rows, cols = np.where(remaining)
    x[rows, cols] = med[cols]
    mean = x.mean(axis=0)
    std = x.std(axis=0)
    std[std == 0] = 1.0
    return (x - mean) / std, missing.sum(axis=0)


if __name__ == "__main__":
    hours, counties, raw = load_table()
    x, missing = standardize(raw)
    print("rows:", len(hours))
    print("distinct hours:", len(set(hours)))
    print("counties:", sorted(set(counties)))
    print("features:", len(FEATURE_NAMES))
    print("rows with no fire detections:", int((raw[:, 0] == 0).sum()))
    per_hour = {}
    for h in hours:
        per_hour[h] = per_hour.get(h, 0) + 1
    print("hours without exactly 5 rows:", sum(1 for c in per_hour.values() if c != 5))
    for name, m in zip(FEATURE_NAMES, missing):
        print("  missing", name + ":", int(m))
    print("standardized shape:", x.shape, "all finite:", bool(np.isfinite(x).all()))
