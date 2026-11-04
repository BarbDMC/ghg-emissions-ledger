"""Fetch emission factors for office electricity (Scope 2) and diesel (Scope 1).

Uses the Climatiq Search API to select a grid-mix electricity factor (Scope 2,
electricity generation) and a stationary diesel combustion factor (Scope 1) per
office country, then resolves each factor's value through the Estimate API
(Search does not expose raw factor values on standard plans). If the API key is
missing, the API is unreachable, the quota is exhausted, or no usable factor is
found, it falls back to bundled reference factors so the pipeline never stops.

Output: ``data/emission_factors.csv``.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "emission_factors.csv"

API_BASE = "https://api.climatiq.io/data/v1"
REQUEST_TIMEOUT = 15

COLUMNS = [
    "factor_id",
    "country_code",
    "energy_type",
    "activity_unit",
    "co2e_factor",
    "source_dataset",
    "factor_year",
    "retrieved_at",
]

# Activity unit used by each office's invoices (see generate_mock_invoices.py).
COUNTRY_DIESEL_UNIT = {"EC": "gallon", "US": "gallon", "GB": "liter", "DE": "liter", "FR": "liter"}
COUNTRIES = list(COUNTRY_DIESEL_UNIT)
ENERGY_CODES = {"electricity": "elec", "diesel": "diesel"}

# Activity unit -> Climatiq Estimate parameters for 1 unit of activity.
ESTIMATE_PARAMETERS = {
    "kWh": {"energy": 1, "energy_unit": "kWh"},
    "liter": {"volume": 1, "volume_unit": "l"},
    "gallon": {"volume": 1, "volume_unit": "gallon_us"},
}

ELECTRICITY_ACTIVITY_ID = "electricity-supply_grid-source_supplier_mix"
# Fuel-specific factors that represent stationary diesel / gas oil combustion.
DIESEL_ACTIVITY_IDS = {
    "fuel-type_diesel-fuel_use_stationary",
    "fuel-type_diesel_fuel-fuel_use_stationary",
    "fuel-type_diesel-fuel_use_na",
    "fuel-type_distillate_fuel_oil_no.2-fuel_use_stationary",
}
# Preferred national authority per country; otherwise the most recent factor wins.
PREFERRED_SOURCES = {"US": "EPA", "GB": "BEIS", "DE": "UBA", "FR": "ADEME"}
# Region whose diesel factor is used when a country has no stationary diesel factor.
DIESEL_PROXY_REGION = {"EC": "US", "FR": "GB", "DE": "GB"}

# Reference factors (kg CO2e per activity unit) used when Climatiq is unavailable.
#  - US electricity: EPA eGRID2023 rev2, US average total output 770.884 lb CO2e/MWh.
#  - GB electricity / diesel (liter): DESNZ/DEFRA 2026 conversion factors
#    (UK electricity generated; Diesel, average biofuel blend).
#  - EC/DE/FR electricity: IEA factors are licensed and not redistributable, so the
#    open Ember dataset (via Our World in Data, 2025 grid average) is used as proxy.
#  - Diesel (gallon): EPA GHG Emission Factors Hub 2025, stationary combustion,
#    Distillate Fuel Oil No. 2 (10.21 kg CO2, 0.41 g CH4, 0.08 g N2O per gal; AR5 GWP).
REFERENCE_FACTORS = {
    ("US", "electricity"): (0.34967, "EPA eGRID2023 (GHG EF Hub 2025)", 2025),
    ("GB", "electricity"): (0.13096, "DESNZ/DEFRA 2026", 2026),
    ("EC", "electricity"): (0.15902, "Ember via OWID (IEA proxy)", 2026),
    ("DE", "electricity"): (0.32965, "Ember via OWID (IEA proxy)", 2026),
    ("FR", "electricity"): (0.04144, "Ember via OWID (IEA proxy)", 2026),
    ("US", "diesel"): (10.243, "EPA GHG EF Hub 2025", 2025),
    ("EC", "diesel"): (10.243, "EPA GHG EF Hub 2025 (global proxy)", 2025),
    ("GB", "diesel"): (2.58354, "DESNZ/DEFRA 2026", 2026),
    ("DE", "diesel"): (2.58354, "DESNZ/DEFRA 2026 (global proxy)", 2026),
    ("FR", "diesel"): (2.58354, "DESNZ/DEFRA 2026 (global proxy)", 2026),
}

log = logging.getLogger("fetch_emission_factors")


class ClimatiqUnavailable(Exception):
    """Raised when the API cannot be used at all (auth, quota, network)."""


@dataclass
class Factor:
    country_code: str
    energy_type: str
    activity_unit: str
    co2e_factor: float
    source_dataset: str
    factor_year: int
    origin: str

    def to_row(self, retrieved_at: str) -> dict:
        return {
            "factor_id": f"{self.origin}_{ENERGY_CODES[self.energy_type]}_"
                         f"{self.country_code.lower()}_{self.factor_year}",
            "country_code": self.country_code,
            "energy_type": self.energy_type,
            "activity_unit": self.activity_unit,
            "co2e_factor": round(self.co2e_factor, 6),
            "source_dataset": self.source_dataset,
            "factor_year": self.factor_year,
            "retrieved_at": retrieved_at,
        }


def target_unit(country_code: str, energy_type: str) -> str:
    return "kWh" if energy_type == "electricity" else COUNTRY_DIESEL_UNIT[country_code]


def reference_factor(country_code: str, energy_type: str) -> Factor:
    value, source, year = REFERENCE_FACTORS[(country_code, energy_type)]
    return Factor(country_code, energy_type, target_unit(country_code, energy_type),
                  value, source, year, origin="ref")


class ClimatiqClient:
    def __init__(self, api_key: str, data_version: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {api_key}"
        retry = Retry(total=3, backoff_factor=1, status_forcelist=(500, 502, 503, 504),
                      allowed_methods=("GET", "POST"))
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.data_version = data_version or self._latest_data_version()

    def _request(self, method: str, path: str, **kwargs) -> Optional[dict]:
        """Return the JSON body, ``None`` for 400 (no match / invalid selection)."""
        try:
            response = self.session.request(method, f"{API_BASE}{path}",
                                            timeout=REQUEST_TIMEOUT, **kwargs)
        except requests.RequestException as exc:
            raise ClimatiqUnavailable(f"connection error: {exc}") from exc
        is_json = response.headers.get("content-type", "").startswith("application/json")
        body = response.json() if is_json else {}
        if response.status_code in (401, 403, 429) or response.status_code >= 500:
            raise ClimatiqUnavailable(
                f"HTTP {response.status_code} {body.get('error_code') or ''} "
                f"{body.get('message') or response.text[:200]}".replace("  ", " ").strip())
        if response.status_code == 400:
            log.debug("Climatiq 400 on %s: %s", path, body.get("message"))
            return None
        response.raise_for_status()
        return body

    def _latest_data_version(self) -> str:
        return f"^{self._request('GET', '/data-versions')['latest_release']}"

    def search(self, **params) -> list[dict]:
        params = {"data_version": self.data_version, "results_per_page": 500, **params}
        return (self._request("GET", "/search", params=params) or {}).get("results", [])

    def estimate(self, factor_id: str, activity_unit: str) -> Optional[float]:
        """kg CO2e for exactly 1 activity unit, i.e. the factor value in that unit."""
        body = self._request("POST", "/estimate", json={
            "emission_factor": {"id": factor_id},
            "parameters": ESTIMATE_PARAMETERS[activity_unit],
        })
        if not body or body.get("co2e_unit") != "kg":
            return None
        return body["co2e"]


def search_candidates(client: ClimatiqClient, region: str, energy_type: str) -> list[dict]:
    if energy_type == "electricity":
        results = client.search(activity_id=ELECTRICITY_ACTIVITY_ID, region=region,
                                scope="2", unit_type="Energy")
        return [r for r in results
                if r.get("activity_id") == ELECTRICITY_ACTIVITY_ID
                and r.get("source_lca_activity") == "electricity_generation"]
    results = client.search(activity_id="fuel-type_di*", region=region, scope="1",
                            unit_type="Volume")
    return [r for r in results
            if r.get("activity_id") in DIESEL_ACTIVITY_IDS
            and r.get("source_lca_activity") == "fuel_combustion"]


def rank(result: dict, region: str) -> tuple:
    preferred = result.get("source") == PREFERRED_SOURCES.get(region)
    return preferred, result.get("year") or 0, result.get("year_released") or 0


def fetch_factor(client: ClimatiqClient, country_code: str, energy_type: str) -> Optional[Factor]:
    regions = [country_code]
    if energy_type == "diesel" and country_code in DIESEL_PROXY_REGION:
        regions.append(DIESEL_PROXY_REGION[country_code])
    unit = target_unit(country_code, energy_type)

    for region in regions:
        candidates = sorted(search_candidates(client, region, energy_type),
                            key=lambda r: rank(r, region), reverse=True)
        for result in candidates[:3]:
            value = client.estimate(result["id"], unit)
            if value is None:
                continue
            proxy = f" ({region} proxy)" if region != country_code else ""
            return Factor(
                country_code=country_code,
                energy_type=energy_type,
                activity_unit=unit,
                co2e_factor=value,
                source_dataset=f"{result.get('source')} - {result.get('source_dataset')}{proxy}",
                factor_year=int(result.get("year_released") or result.get("year")),
                origin="climatiq",
            )
    return None


def collect_factors(api_key: Optional[str], data_version: Optional[str]) -> list[Factor]:
    pairs = [(country, energy) for country in COUNTRIES for energy in ENERGY_CODES]
    if not api_key:
        log.warning("CLIMATIQ_API_KEY is not set. Using reference factors (EPA, DESNZ/DEFRA, Ember).")
        return [reference_factor(*pair) for pair in pairs]

    try:
        client = ClimatiqClient(api_key, data_version)
        log.info("Querying Climatiq (data_version=%s)", client.data_version)
        factors = []
        for country, energy in pairs:
            factor = fetch_factor(client, country, energy)
            if factor is None:
                log.warning("No usable Climatiq factor for %s/%s. Using reference factor.",
                            country, energy)
                factor = reference_factor(country, energy)
            factors.append(factor)
        return factors
    except ClimatiqUnavailable as exc:
        log.warning("Climatiq API unavailable (%s). Using reference factors for all rows.", exc)
        return [reference_factor(*pair) for pair in pairs]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--offline", action="store_true",
                        help="Skip the API and write reference factors only.")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    api_key = None if args.offline else (os.getenv("CLIMATIQ_API_KEY") or "").strip() or None
    factors = collect_factors(api_key, os.getenv("CLIMATIQ_DATA_VERSION") or None)

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = pd.DataFrame([f.to_row(retrieved_at) for f in factors], columns=COLUMNS)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)

    origins = pd.Series([f.origin for f in factors]).value_counts().to_dict()
    log.info("Wrote %d factors to %s (%s)", len(df), args.output, origins)


if __name__ == "__main__":
    main()
