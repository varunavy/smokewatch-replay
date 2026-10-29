"""Validate the saved SmokeWatch Replay county-hour batch reference.

The project data files are intentionally excluded from Git. Run this script
after executing the full-period pipeline notebook on the machine containing
the local data directory.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


EXPECTED_COUNTIES = {"Butte", "Lassen", "Plumas", "Shasta", "Tehama"}
EXPECTED_ROWS = 6_000
EXPECTED_HOURS = 1_200
KEY_COLUMNS = ["event_hour_utc", "county"]

FILE_SPECS = {
    "nasa_firms": {
        "relative_path": (
            "data/interim/nasa_firms/"
            "dixie_fire_hourly_county_full_2021-07-13_2021-08-31.csv"
        ),
        "expected_columns": 8,
    },
    "epa_pm25": {
        "relative_path": (
            "data/interim/epa_airdata/"
            "dixie_pm25_hourly_county_full_2021-07-13_2021-08-31.csv"
        ),
        "expected_columns": 9,
    },
    "noaa_weather": {
        "relative_path": (
            "data/interim/noaa_ghcnh/"
            "dixie_weather_hourly_county_full_2021-07-13_2021-08-31.csv"
        ),
        "expected_columns": 14,
    },
    "integrated": {
        "relative_path": (
            "data/interim/integrated/"
            "dixie_integrated_hourly_county_full_2021-07-13_2021-08-31.csv"
        ),
        "expected_columns": 27,
    },
}


def add_check(
    checks: list[dict[str, object]],
    dataset: str,
    check: str,
    observed: object,
    expected: object,
    passed: bool,
) -> None:
    """Append one validation result."""
    checks.append(
        {
            "dataset": dataset,
            "check": check,
            "observed": observed,
            "expected": expected,
            "status": "PASS" if passed else "FAIL",
        }
    )


def load_dataset(project_root: Path, dataset: str) -> pd.DataFrame:
    """Load one configured dataset and normalize its key columns."""
    path = project_root / FILE_SPECS[dataset]["relative_path"]
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {dataset} file: {path}\n"
            "Run notebooks/01_full_period_pipeline.ipynb first."
        )

    frame = pd.read_csv(path)
    missing_keys = set(KEY_COLUMNS).difference(frame.columns)
    if missing_keys:
        raise ValueError(f"{dataset} is missing key columns: {missing_keys}")

    frame["event_hour_utc"] = pd.to_datetime(
        frame["event_hour_utc"], utc=True, errors="coerce"
    )
    frame["county"] = frame["county"].astype("string").str.strip()
    return frame


def validate_structure(
    dataset: str,
    frame: pd.DataFrame,
    checks: list[dict[str, object]],
) -> None:
    """Validate the common county-hour structure."""
    county_counts = frame.groupby("county").size()
    observed_counties = set(frame["county"].dropna().astype(str))

    add_check(
        checks,
        dataset,
        "row_count",
        len(frame),
        EXPECTED_ROWS,
        len(frame) == EXPECTED_ROWS,
    )
    add_check(
        checks,
        dataset,
        "column_count",
        len(frame.columns),
        FILE_SPECS[dataset]["expected_columns"],
        len(frame.columns) == FILE_SPECS[dataset]["expected_columns"],
    )
    add_check(
        checks,
        dataset,
        "unique_utc_hours",
        frame["event_hour_utc"].nunique(),
        EXPECTED_HOURS,
        frame["event_hour_utc"].nunique() == EXPECTED_HOURS,
    )
    add_check(
        checks,
        dataset,
        "county_set",
        ", ".join(sorted(observed_counties)),
        ", ".join(sorted(EXPECTED_COUNTIES)),
        observed_counties == EXPECTED_COUNTIES,
    )
    add_check(
        checks,
        dataset,
        "duplicate_county_hour_keys",
        int(frame.duplicated(KEY_COLUMNS).sum()),
        0,
        not frame.duplicated(KEY_COLUMNS).any(),
    )
    add_check(
        checks,
        dataset,
        "invalid_timestamps",
        int(frame["event_hour_utc"].isna().sum()),
        0,
        frame["event_hour_utc"].notna().all(),
    )
    add_check(
        checks,
        dataset,
        "hours_per_county",
        ", ".join(f"{k}:{v}" for k, v in county_counts.items()),
        "1200 for each county",
        county_counts.eq(EXPECTED_HOURS).all(),
    )


def validate_metric(
    dataset: str,
    frame: pd.DataFrame,
    column: str,
    expected_total: int,
    checks: list[dict[str, object]],
) -> None:
    """Validate one numeric or Boolean total when the column is present."""
    if column not in frame.columns:
        add_check(checks, dataset, f"{column}_total", "missing", expected_total, False)
        return

    values = frame[column]
    if not pd.api.types.is_numeric_dtype(values):
        normalized = values.astype("string").str.lower().map(
            {"true": 1, "false": 0}
        )
        values = normalized

    observed_total = int(pd.to_numeric(values, errors="coerce").fillna(0).sum())
    add_check(
        checks,
        dataset,
        f"{column}_total",
        observed_total,
        expected_total,
        observed_total == expected_total,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    default_root = Path(__file__).resolve().parents[2]
    parser.add_argument(
        "--project-root",
        type=Path,
        default=default_root,
        help="SmokeWatch Replay repository root",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional validation CSV output path",
    )
    args = parser.parse_args()

    project_root = args.project_root.resolve()
    output_path = args.output or (
        project_root / "reports/tables/batch_validation_summary.csv"
    )

    frames: dict[str, pd.DataFrame] = {}
    checks: list[dict[str, object]] = []

    for dataset in FILE_SPECS:
        frames[dataset] = load_dataset(project_root, dataset)
        validate_structure(dataset, frames[dataset], checks)

    reference_keys = set(
        map(tuple, frames["integrated"][KEY_COLUMNS].astype(str).to_numpy())
    )
    for dataset in ("nasa_firms", "epa_pm25", "noaa_weather"):
        source_keys = set(
            map(tuple, frames[dataset][KEY_COLUMNS].astype(str).to_numpy())
        )
        add_check(
            checks,
            dataset,
            "keys_match_integrated_reference",
            len(source_keys.intersection(reference_keys)),
            EXPECTED_ROWS,
            source_keys == reference_keys,
        )

    validate_metric(
        "nasa_firms", frames["nasa_firms"], "fire_detection_count", 53_394, checks
    )
    validate_metric(
        "epa_pm25", frames["epa_pm25"], "pm25_available", 4_791, checks
    )
    validate_metric(
        "noaa_weather", frames["noaa_weather"], "weather_available", 5_804, checks
    )
    validate_metric(
        "integrated", frames["integrated"], "fire_detection_count", 53_394, checks
    )
    validate_metric(
        "integrated", frames["integrated"], "pm25_available", 4_791, checks
    )
    validate_metric(
        "integrated", frames["integrated"], "weather_available", 5_804, checks
    )

    summary = pd.DataFrame(checks)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_path, index=False)

    print(summary.to_string(index=False))
    print(f"\nValidation summary written to: {output_path}")

    failures = summary.loc[summary["status"].eq("FAIL")]
    if not failures.empty:
        raise AssertionError(f"{len(failures)} batch-reference validation checks failed")

    print("All batch-reference validation checks passed.")


if __name__ == "__main__":
    main()
