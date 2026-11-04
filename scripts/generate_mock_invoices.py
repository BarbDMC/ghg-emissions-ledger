"""Generate synthetic corporate energy invoices for the GHG emissions ledger.

Produces 12 months of electricity and diesel invoices for each office and writes
one CSV per office into ``data/mock_invoices/``. It also injects a business test
case: an invoice re-issued 15 days later with the same ``invoice_id`` and a
corrected ``usage_amount`` (to exercise downstream deduplication).

Units and ISO country codes are chosen so they can later be mapped to Climatiq
activity parameters (``energy`` in kWh, ``volume`` in L / US gal).
"""

from __future__ import annotations

import argparse
import calendar
import os
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from faker import Faker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "mock_invoices"

COLUMNS = [
    "invoice_id",
    "office_id",
    "country_code",
    "issue_date",
    "consumption_start_date",
    "consumption_end_date",
    "energy_type",
    "usage_amount",
    "usage_unit",
    "cost_amount",
    "currency",
]

ISSUE_LAG_DAYS = 5
CORRECTION_LAG_DAYS = 15

USAGE_BOUNDS = {"electricity": (5_000.0, 50_000.0)}


@dataclass(frozen=True)
class EnergyProfile:
    unit: str
    base_monthly_usage: float
    unit_price: float


@dataclass(frozen=True)
class Office:
    office_id: str
    city: str
    country_code: str
    currency: str
    electricity: EnergyProfile
    diesel: EnergyProfile

    def profile(self, energy_type: str) -> EnergyProfile:
        return getattr(self, energy_type)


OFFICES: list[Office] = [
    Office("OFF_UIO", "Quito", "EC", "USD",
           EnergyProfile("kWh", 18_000, 0.095), EnergyProfile("gal", 120, 1.85)),
    Office("OFF_NYC", "New York", "US", "USD",
           EnergyProfile("kWh", 38_000, 0.230), EnergyProfile("gal", 90, 4.20)),
    Office("OFF_LON", "London", "GB", "GBP",
           EnergyProfile("kWh", 35_000, 0.270), EnergyProfile("L", 380, 1.55)),
    Office("OFF_FRA", "Frankfurt", "DE", "EUR",
           EnergyProfile("kWh", 30_000, 0.320), EnergyProfile("L", 300, 1.75)),
    Office("OFF_PAR", "Paris", "FR", "EUR",
           EnergyProfile("kWh", 28_000, 0.250), EnergyProfile("L", 260, 1.85)),
]

ENERGY_TYPES = ("electricity", "diesel")
ENERGY_CODES = {"electricity": "ELE", "diesel": "DSL"}


def month_periods(end_year: int, end_month: int, months: int) -> list[tuple[date, date]]:
    """Return ``months`` consecutive (start, end) periods ending at end_year/end_month."""
    periods = []
    year, month = end_year, end_month
    for _ in range(months):
        last_day = calendar.monthrange(year, month)[1]
        periods.append((date(year, month, 1), date(year, month, last_day)))
        year, month = (year - 1, 12) if month == 1 else (year, month - 1)
    return list(reversed(periods))


def seasonal_factor(office: Office, energy_type: str, month: int) -> float:
    """Simple seasonality: northern-hemisphere offices consume more in winter/summer peaks."""
    if office.country_code == "EC":  # equatorial climate, flat profile
        return 1.0
    if energy_type == "electricity":
        return {1: 1.15, 2: 1.10, 6: 1.05, 7: 1.15, 8: 1.12, 12: 1.18}.get(month, 1.0)
    return {1: 1.25, 2: 1.20, 12: 1.30}.get(month, 1.0)


def clamp_usage(energy_type: str, usage: float) -> float:
    low, high = USAGE_BOUNDS.get(energy_type, (0.0, float("inf")))
    return round(min(max(usage, low), high), 2)


def build_invoice(fake: Faker, rng: random.Random, office: Office, energy_type: str,
                  start: date, end: date) -> dict:
    profile = office.profile(energy_type)
    usage = profile.base_monthly_usage * seasonal_factor(office, energy_type, start.month)
    usage = clamp_usage(energy_type, usage * rng.uniform(0.88, 1.12))
    cost = round(usage * profile.unit_price * rng.uniform(0.97, 1.03), 2)
    invoice_id = (
        f"INV-{office.office_id.split('_')[1]}-{ENERGY_CODES[energy_type]}-"
        f"{start:%Y%m}-{fake.unique.bothify('????##').upper()}"
    )
    return {
        "invoice_id": invoice_id,
        "office_id": office.office_id,
        "country_code": office.country_code,
        "issue_date": end + timedelta(days=ISSUE_LAG_DAYS),
        "consumption_start_date": start,
        "consumption_end_date": end,
        "energy_type": energy_type,
        "usage_amount": usage,
        "usage_unit": profile.unit,
        "cost_amount": cost,
        "currency": office.currency,
    }


def build_correction(original: dict, rng: random.Random) -> dict:
    """Re-issue an invoice 15 days later with a corrected usage (same invoice_id)."""
    corrected = dict(original)
    factor = rng.choice([rng.uniform(0.80, 0.92), rng.uniform(1.08, 1.20)])
    unit_cost = original["cost_amount"] / original["usage_amount"]
    corrected["usage_amount"] = clamp_usage(original["energy_type"], original["usage_amount"] * factor)
    corrected["cost_amount"] = round(corrected["usage_amount"] * unit_cost, 2)
    corrected["issue_date"] = original["issue_date"] + timedelta(days=CORRECTION_LAG_DAYS)
    return corrected


def generate(end_year: int, end_month: int, months: int, seed: int,
             corrections: int) -> pd.DataFrame:
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)

    rows = [
        build_invoice(fake, rng, office, energy_type, start, end)
        for office in OFFICES
        for energy_type in ENERGY_TYPES
        for start, end in month_periods(end_year, end_month, months)
    ]
    rows += [build_correction(original, rng) for original in rng.sample(rows, corrections)]

    df = pd.DataFrame(rows, columns=COLUMNS)
    return df.sort_values(["office_id", "energy_type", "consumption_start_date", "issue_date"],
                          ignore_index=True)


def write_csvs(df: pd.DataFrame, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for office_id, office_df in df.groupby("office_id"):
        path = output_dir / f"invoices_{office_id}.csv"
        office_df.to_csv(path, index=False, date_format="%Y-%m-%d")
        paths.append(path)
    return paths


def parse_args() -> argparse.Namespace:
    today = date.today()
    last_month = today.replace(day=1) - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--end-year", type=int, default=last_month.year)
    parser.add_argument("--end-month", type=int, default=last_month.month)
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--seed", type=int, default=int(os.getenv("MOCK_SEED", 42)))
    parser.add_argument("--corrections", type=int, default=1,
                        help="Number of invoices re-issued with a corrected usage_amount.")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(os.getenv("MOCK_INVOICES_DIR", DEFAULT_OUTPUT_DIR)))
    return parser.parse_args()


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    df = generate(args.end_year, args.end_month, args.months, args.seed, args.corrections)
    paths = write_csvs(df, args.output_dir)

    dupes = df[df.duplicated("invoice_id", keep=False)]
    print(f"Generated {len(df)} invoices across {len(paths)} files in {args.output_dir}")
    for path in paths:
        print(f"  - {path.relative_to(PROJECT_ROOT) if path.is_relative_to(PROJECT_ROOT) else path}")
    print(f"Duplicate invoice_id rows (dedup test case): {len(dupes)}")


if __name__ == "__main__":
    main()
