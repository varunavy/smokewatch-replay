#!/usr/bin/env python3
"""Evaluate a Laplace differential-privacy mechanism on FIRMS activity.

The query releases the number of active fire-detection hours for every
county-day. A county-hour contributes 1 when fire_detection_count > 0 and 0
otherwise. Under record-level adjacency, changing one county-hour changes the
entire released count vector by at most 1, so its L1 sensitivity is 1.

Repeated trials in this script are used only to evaluate utility. Publishing
many independently noised versions of the same query would consume additional
privacy budget and is not what this experiment recommends.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from collections import defaultdict
from pathlib import Path
from typing import Iterable


DEFAULT_INPUT = Path(
    "data/processed/replay/"
    "nasa_firms_events_full_2021-07-13_2021-08-31.csv"
)
DEFAULT_OUTPUT = Path("experiments/differential_privacy/results.csv")
EPSILONS = (0.1, 0.5, 1.0, 2.0, 5.0)
SENSITIVITY = 1.0
MIN_DAILY_HOURS = 0.0
MAX_DAILY_HOURS = 24.0
EXPECTED_COLUMNS = {
    "county",
    "event_hour_utc",
    "fire_detection_count",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure the privacy-utility tradeoff when releasing differentially "
            "private daily fire-active-hour counts."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--trials", type=int, default=200)
    parser.add_argument("--seed", type=int, default=226)
    return parser.parse_args()


def load_daily_active_hours(path: Path) -> tuple[dict[tuple[str, str], int], dict[str, int]]:
    """Load FIRMS rows and calculate exact active hours for each county-day."""
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path}")

    daily_active_hours: defaultdict[tuple[str, str], int] = defaultdict(int)
    rows_per_group: defaultdict[tuple[str, str], int] = defaultdict(int)
    counties: set[str] = set()
    dates: set[str] = set()
    rows = 0
    active_rows = 0

    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        missing_columns = EXPECTED_COLUMNS.difference(reader.fieldnames or [])
        if missing_columns:
            names = ", ".join(sorted(missing_columns))
            raise ValueError(f"Input is missing required columns: {names}")

        for line_number, row in enumerate(reader, start=2):
            rows += 1
            county = row["county"].strip()
            event_hour = row["event_hour_utc"].strip()
            if not county or len(event_hour) < 10:
                raise ValueError(f"Invalid county or timestamp on line {line_number}")

            try:
                detection_count = float(row["fire_detection_count"])
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"Invalid fire_detection_count on line {line_number}"
                ) from error

            if not math.isfinite(detection_count) or detection_count < 0:
                raise ValueError(
                    f"fire_detection_count must be finite and nonnegative on line {line_number}"
                )
            if not detection_count.is_integer():
                raise ValueError(
                    f"fire_detection_count must be an integer on line {line_number}"
                )

            date = event_hour[:10]
            key = (county, date)
            active = int(detection_count > 0)
            daily_active_hours[key] += active
            rows_per_group[key] += 1
            active_rows += active
            counties.add(county)
            dates.add(date)

    metadata = {
        "rows": rows,
        "counties": len(counties),
        "dates": len(dates),
        "groups": len(daily_active_hours),
        "active_rows": active_rows,
    }

    if rows != 6000:
        raise ValueError(f"Expected 6000 input rows, found {rows}")
    if len(counties) != 5 or len(dates) != 50 or len(daily_active_hours) != 250:
        raise ValueError(
            "Expected 5 counties, 50 dates and 250 county-day groups; "
            f"found {len(counties)}, {len(dates)} and {len(daily_active_hours)}"
        )
    incomplete = [key for key, count in rows_per_group.items() if count != 24]
    if incomplete:
        raise ValueError(f"Expected 24 county-hour rows per group; found {len(incomplete)} incomplete groups")
    if any(not 0 <= value <= 24 for value in daily_active_hours.values()):
        raise ValueError("An exact daily active-hour count is outside the valid range 0 to 24")

    return dict(daily_active_hours), metadata


def laplace_noise(rng: random.Random, scale: float) -> float:
    """Draw zero-centered Laplace noise using inverse-transform sampling."""
    uniform = rng.random() - 0.5
    sign = 1.0 if uniform >= 0 else -1.0
    return -scale * sign * math.log1p(-2.0 * abs(uniform))


def percentile(values: Iterable[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("Cannot calculate a percentile of an empty sequence")
    index = max(0, math.ceil(probability * len(ordered)) - 1)
    return ordered[index]


def evaluate_epsilon(
    exact_counts: dict[tuple[str, str], int],
    epsilon: float,
    trials: int,
    seed: int,
) -> dict[str, float | int]:
    """Simulate the mechanism and summarize error after valid-range clipping."""
    if epsilon <= 0:
        raise ValueError("epsilon must be positive")
    if trials <= 0:
        raise ValueError("trials must be positive")

    scale = SENSITIVITY / epsilon
    rng = random.Random(seed + round(epsilon * 1000))
    absolute_errors: list[float] = []
    squared_errors: list[float] = []
    signed_errors: list[float] = []
    clipped_releases = 0

    for _ in range(trials):
        for exact in exact_counts.values():
            raw_private = exact + laplace_noise(rng, scale)
            private = min(MAX_DAILY_HOURS, max(MIN_DAILY_HOURS, raw_private))
            if private != raw_private:
                clipped_releases += 1
            error = private - exact
            absolute_errors.append(abs(error))
            squared_errors.append(error * error)
            signed_errors.append(error)

    releases = len(absolute_errors)
    return {
        "epsilon": epsilon,
        "sensitivity": SENSITIVITY,
        "noise_scale": scale,
        "trials": trials,
        "county_day_groups": len(exact_counts),
        "evaluated_releases": releases,
        "mean_absolute_error": sum(absolute_errors) / releases,
        "root_mean_squared_error": math.sqrt(sum(squared_errors) / releases),
        "p95_absolute_error": percentile(absolute_errors, 0.95),
        "mean_signed_error": sum(signed_errors) / releases,
        "clipped_fraction": clipped_releases / releases,
        "theoretical_unclipped_mae": scale,
    }


def write_results(path: Path, results: list[dict[str, float | int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(results[0]), lineterminator="\n")
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    key: f"{value:.6f}" if isinstance(value, float) else value
                    for key, value in result.items()
                }
            )


def main() -> None:
    args = parse_args()
    exact_counts, metadata = load_daily_active_hours(args.input)
    results = [
        evaluate_epsilon(exact_counts, epsilon, args.trials, args.seed)
        for epsilon in EPSILONS
    ]
    write_results(args.output, results)

    exact_values = list(exact_counts.values())
    print("Differential privacy experiment")
    print(f"input={args.input}")
    print(
        "validation="
        f"rows:{metadata['rows']} counties:{metadata['counties']} "
        f"dates:{metadata['dates']} groups:{metadata['groups']} "
        f"active_county_hours:{metadata['active_rows']}"
    )
    print(
        f"query=daily fire-active hours exact_range:{min(exact_values)}-{max(exact_values)} "
        f"sensitivity:{SENSITIVITY:g} valid_range:0-24"
    )
    print(f"trials={args.trials} seed={args.seed}")
    for result in results:
        print(
            f"epsilon={result['epsilon']:.1f} "
            f"scale={result['noise_scale']:.3f} "
            f"mae={result['mean_absolute_error']:.3f} "
            f"rmse={result['root_mean_squared_error']:.3f} "
            f"p95_abs_error={result['p95_absolute_error']:.3f} "
            f"clipped_fraction={result['clipped_fraction']:.3f}"
        )
    print(f"results={args.output}")
    print("note=trials evaluate utility only; repeated publication would consume privacy budget")


if __name__ == "__main__":
    main()
