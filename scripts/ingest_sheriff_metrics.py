"""Session 2 ingestion: Sheriff accountability metrics for all 66 FL Sheriffs.

Pulls real performance data from public sources where available:

  crime_clearance_rate    FBI Crime Data Explorer (api.usa.gov /summarized)
  officer_involved_shootings  Washington Post fatal-police-shootings dataset
  use_of_force_complaints     SKIPPED — no public agency-level FL feed
  jail_deaths                 SKIPPED — BJS data is state-aggregate PDF only
  lawsuits_settled            SKIPPED — PACER is paywalled, no batch API

For the skipped metrics we log a row to scrape_failures so it's visible
why each sheriff doesn't have that metric.

Pre-reqs (run scripts to download data once):
  data/wapo/agencies.csv
  data/wapo/fatal_shootings.csv

Env:
  DATA_GOV_API_KEY  api.data.gov key for FBI CDE
  SUPABASE_URL, SUPABASE_SERVICE_KEY

Usage:
  python scripts/ingest_sheriff_metrics.py
  python scripts/ingest_sheriff_metrics.py --ids 427,471,476,478   # spot check
  python scripts/ingest_sheriff_metrics.py --limit 5
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

LOG = logging.getLogger("sheriff_metrics")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest_sheriff_metrics.log", mode="a"),
    ],
)

DATA_GOV_KEY = os.getenv("DATA_GOV_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

CDE_BASE = "https://api.usa.gov/crime/fbi/cde"
CENSUS_FL_POP_URL = (
    "https://api.census.gov/data/2023/acs/acs5?get=NAME,B01003_001E"
    "&for=county:*&in=state:12"
)
WAPO_AGENCIES = "data/wapo/agencies.csv"
WAPO_SHOOTINGS = "data/wapo/fatal_shootings.csv"

CDE_DELAY = 1.1  # ~1 req/sec (api.data.gov default 1000/hr)
CLEARANCE_YEAR = 2023  # Year stamp for clearance metric
CLEARANCE_FROM = "01-2022"  # Two-year window smooths sparse NIBRS reporting
CLEARANCE_TO = "12-2023"
CLEARANCE_MIN_OFFENSES = 100  # Below this we log insufficient_data
OIS_YEAR = 2024  # Most recent year with WaPo data
SKIPPED_METRICS_YEAR = OIS_YEAR

session = requests.Session()
session.headers.update({"User-Agent": "PolitiScore Civic Data Bot +https://politiscore.com"})


# --- Rating helpers --------------------------------------------------------


def rate_clearance(pct: float) -> str:
    if pct > 50:
        return "excellent"
    if pct >= 40:
        return "good"
    if pct >= 30:
        return "meeting"
    if pct >= 20:
        return "concerning"
    return "poor"


def rate_ois(rate_per_100k: float) -> str:
    """Per-year OIS per 100K residents."""
    if rate_per_100k == 0:
        return "excellent"
    if rate_per_100k <= 1:
        return "good"
    if rate_per_100k <= 2:
        return "meeting"
    if rate_per_100k <= 3:
        return "concerning"
    return "poor"


# --- HTTP helper ----------------------------------------------------------


def http_get(url: str, params: dict | None = None, timeout: int = 25, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = 2 ** attempt * 5
                LOG.warning("429 rate-limited, sleeping %ds", wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = 2 ** attempt * 2
                LOG.warning("HTTP %d, retry in %ds: %s", r.status_code, wait, url[:100])
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            LOG.warning("HTTP error attempt %d: %s — %s", attempt + 1, url[:100], e)
            time.sleep(2)
    return None


# --- Source loaders --------------------------------------------------------


_county_pop_cache: dict[str, int] | None = None


def fetch_fl_county_populations() -> dict[str, int]:
    """county_name (lowercased, no 'county' suffix) -> population."""
    global _county_pop_cache
    if _county_pop_cache is not None:
        return _county_pop_cache
    d = http_get(CENSUS_FL_POP_URL)
    if not d:
        LOG.error("Census FL population fetch failed")
        return {}
    out: dict[str, int] = {}
    for row in d[1:]:
        name = row[0]  # "Alachua County, Florida"
        pop = int(row[1])
        m = re.match(r"^(.+?)\s+County,\s+Florida$", name)
        if m:
            key = _county_key(m.group(1))
            out[key] = pop
    _county_pop_cache = out
    LOG.info("Cached %d FL county populations", len(out))
    return out


def _county_key(s: str) -> str:
    return re.sub(r"[^a-z]", "", s.lower())


_wapo_cache: dict | None = None


def load_wapo_data() -> dict:
    """Return {agencies_by_id, fl_sheriff_agencies (list), shootings_by_agency_year}."""
    global _wapo_cache
    if _wapo_cache is not None:
        return _wapo_cache

    agencies_by_id: dict[int, dict] = {}
    fl_sheriff_agencies: list[dict] = []
    with open(WAPO_AGENCIES, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                aid = int(row["id"])
            except ValueError:
                continue
            agencies_by_id[aid] = row
            if row.get("state") == "FL" and row.get("type") == "sheriff":
                fl_sheriff_agencies.append(row)

    shootings_by_agency_year: dict[tuple[int, int], int] = defaultdict(int)
    with open(WAPO_SHOOTINGS, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("state") != "FL":
                continue
            date = row.get("date", "")
            if not date:
                continue
            try:
                year = int(date[:4])
            except ValueError:
                continue
            for raw in (row.get("agency_ids") or "").split(";"):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    aid = int(raw)
                except ValueError:
                    continue
                shootings_by_agency_year[(aid, year)] += 1

    _wapo_cache = {
        "agencies_by_id": agencies_by_id,
        "fl_sheriff_agencies": fl_sheriff_agencies,
        "shootings_by_agency_year": shootings_by_agency_year,
    }
    LOG.info(
        "Loaded WaPo data: %d FL sheriff agencies, %d (agency,year) shooting buckets",
        len(fl_sheriff_agencies),
        len(shootings_by_agency_year),
    )
    return _wapo_cache


def find_wapo_sheriff(county_name: str) -> dict | None:
    wd = load_wapo_data()
    target_key = _county_key(county_name)
    for ag in wd["fl_sheriff_agencies"]:
        # name is like "Alachua County Sheriff's Office"
        name = ag.get("name", "")
        m = re.match(r"^(.+?)\s+County\s+Sheriff", name, re.I)
        if not m:
            continue
        if _county_key(m.group(1)) == target_key:
            return ag
    return None


def find_fbi_ori_for_sheriff(county_name: str) -> str | None:
    """Look up FL sheriff ORI from WaPo agencies file (oricodes column)."""
    ag = find_wapo_sheriff(county_name)
    if not ag:
        return None
    ori = (ag.get("oricodes") or "").strip()
    # WaPo stores 7-char ORI like "FL00100"; FBI CDE uses 9 chars "FL0010000"
    if not ori:
        return None
    # Pad with zeros to 9 chars
    if len(ori) == 7:
        ori = ori + "00"
    return ori


_cde_fl_agencies_cache: dict | None = None


def _fetch_cde_fl_agencies() -> dict:
    """{COUNTY_UPPER -> [agency dict]} from FBI CDE byStateAbbr endpoint."""
    global _cde_fl_agencies_cache
    if _cde_fl_agencies_cache is not None:
        return _cde_fl_agencies_cache
    d = http_get(f"{CDE_BASE}/agency/byStateAbbr/FL", params={"api_key": DATA_GOV_KEY})
    time.sleep(CDE_DELAY)
    _cde_fl_agencies_cache = d or {}
    LOG.info("Cached %d FL counties from FBI CDE agency listing", len(_cde_fl_agencies_cache))
    return _cde_fl_agencies_cache


def find_fbi_ori_via_cde(county_name: str) -> str | None:
    """Fallback ORI lookup directly from FBI CDE agency listing."""
    data = _fetch_cde_fl_agencies()
    target = _county_key(county_name)
    for county_key, agencies in data.items():
        if _county_key(county_key) != target:
            continue
        for ag in agencies:
            agency_name = (ag.get("agency_name") or "").lower()
            agency_type = (ag.get("agency_type_name") or "").lower()
            if "sheriff" in agency_name and agency_type == "county":
                return ag.get("ori")
    return None


# --- Metric pulls ---------------------------------------------------------


def _agency_totals(d: dict) -> tuple[int, int]:
    """Sum offenses and clearances for the agency series in a CDE summarized response."""
    actuals = (d.get("offenses") or {}).get("actuals") or {}
    off_total = 0
    cle_total = 0
    for k, v in actuals.items():
        kl = k.lower()
        if "florida" in kl or "united states" in kl:
            continue
        if not isinstance(v, dict):
            continue
        s = sum((x or 0) for x in v.values())
        if kl.endswith("offenses"):
            off_total += s
        elif kl.endswith("clearances"):
            cle_total += s
    return off_total, cle_total


def fetch_clearance(ori: str) -> tuple[float, int, int] | None:
    """Combined violent + property crime clearance over CLEARANCE_FROM..CLEARANCE_TO.

    Returns (pct, total_offenses, total_clearances) or None if data is too sparse.
    """
    total_off = 0
    total_cle = 0
    for offense_type in ("violent-crime", "property-crime"):
        d = http_get(
            f"{CDE_BASE}/summarized/agency/{ori}/{offense_type}",
            params={"from": CLEARANCE_FROM, "to": CLEARANCE_TO, "api_key": DATA_GOV_KEY},
        )
        time.sleep(CDE_DELAY)
        if not d:
            continue
        off, cle = _agency_totals(d)
        total_off += off
        total_cle += cle

    if total_off < CLEARANCE_MIN_OFFENSES:
        return None
    pct = round(total_cle / total_off * 100, 1)
    return pct, total_off, total_cle


def fetch_ois_for_year(wapo_agency_id: int, year: int) -> int:
    wd = load_wapo_data()
    return wd["shootings_by_agency_year"].get((wapo_agency_id, year), 0)


# --- Persistence ----------------------------------------------------------


def upsert_metric(supabase, official_id: int, official_name: str, **fields) -> None:
    row = {
        "official_id": official_id,
        "official_name": official_name,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    supabase.table("accountability_metrics").upsert(
        row, on_conflict="official_id,metric_key,year"
    ).execute()


def log_failure(supabase, official_id: int, identifier: str, reason: str, url: str | None = None) -> None:
    try:
        supabase.table("scrape_failures").insert(
            {
                "source_table": "accountability_metrics",
                "source_id": official_id,
                "identifier": identifier[:200],
                "reason": reason[:500],
                "url": url,
            }
        ).execute()
    except Exception as e:
        LOG.warning("scrape_failures insert failed: %s", e)


# --- Per-sheriff processing -----------------------------------------------


SKIP_METRIC_DEFS = [
    (
        "use_of_force_complaints",
        "Use-of-Force Complaints",
        "%",
        "FL has no public agency-level UoF database; FDLE Annual UoF report is PDF state-aggregate only. Manual public-records request required.",
    ),
    (
        "jail_deaths",
        "In-Custody Deaths",
        "deaths/year",
        "BJS Mortality in Local Jails publishes only state-aggregate PDF; per-county jail death data requires FOIA to each county sheriff.",
    ),
    (
        "lawsuits_settled",
        "Lawsuits Settled",
        "$",
        "PACER per-document fees prevent automated retrieval; FL state court civil records require county-by-county scraping.",
    ),
]


def process_sheriff(supabase, off: dict) -> tuple[int, list[str]]:
    """Return (n_filled, list of source labels hit)."""
    name = off["name"]
    oid = off["id"]
    title = off.get("title") or ""

    m = re.match(r"Sheriff,\s*(.+?)\s*County", title)
    if not m:
        m = re.match(r"^(.+?)\s+County\s+Sheriff", title)
    if not m:
        LOG.warning("Could not parse county from title %r", title)
        log_failure(supabase, oid, name, f"Could not parse county from title: {title!r}")
        return 0, []
    county = m.group(1).strip()

    sources_hit: list[str] = []
    filled = 0

    # 1. Crime clearance rate via FBI CDE
    ori = find_fbi_ori_for_sheriff(county)
    if not ori:
        ori = find_fbi_ori_via_cde(county)
    if ori:
        try:
            res = fetch_clearance(ori)
            if res:
                pct, total_off, total_cle = res
                upsert_metric(
                    supabase,
                    oid,
                    name,
                    metric_key="crime_clearance_rate",
                    metric_label="Case Clearance Rate",
                    metric_value=str(pct),
                    metric_unit="%",
                    benchmark_value="38.4",
                    benchmark_label="FL state average",
                    performance_rating=rate_clearance(pct),
                    year=CLEARANCE_YEAR,
                    source="FBI Crime Data Explorer",
                    source_url=f"https://cde.ucr.cjis.gov/LATEST/webapp/agency/{ori}/crime",
                    notes=(
                        f"Violent + property crime: {total_cle} cleared of {total_off} offenses "
                        f"(FBI UCR {CLEARANCE_FROM}..{CLEARANCE_TO})"
                    ),
                )
                filled += 1
                sources_hit.append("FBI CDE")
            else:
                log_failure(
                    supabase,
                    oid,
                    name,
                    (
                        f"FBI CDE clearance: insufficient data for ORI {ori} "
                        f"(need >= {CLEARANCE_MIN_OFFENSES} offenses {CLEARANCE_FROM}..{CLEARANCE_TO})"
                    ),
                    f"{CDE_BASE}/summarized/agency/{ori}/violent-crime",
                )
        except Exception as e:
            LOG.warning("Clearance fetch failed for %s: %s", name, e)
            log_failure(supabase, oid, name, f"FBI CDE clearance fetch error: {e}")
    else:
        log_failure(supabase, oid, name, f"No FBI ORI mapped for {county} County Sheriff")

    # 2. OIS via WaPo dataset.
    # WaPo only includes agencies with at least one historical fatal shooting,
    # so absence from WaPo means the agency has 0 fatal OIS recorded.
    wapo_ag = find_wapo_sheriff(county)
    aid = int(wapo_ag["id"]) if wapo_ag else None
    count = fetch_ois_for_year(aid, OIS_YEAR) if aid else 0
    try:
        pop = fetch_fl_county_populations().get(_county_key(county))
        if pop:
            rate = round(count / pop * 100_000, 2)
            rating = rate_ois(rate)
            notes = (
                f"{count} fatal officer-involved shootings in {OIS_YEAR} "
                f"(WaPo dataset; county population {pop:,}; rate {rate}/100K)"
            )
        else:
            rate = None
            rating = "good" if count == 0 else "meeting" if count <= 2 else "concerning"
            notes = (
                f"{count} fatal officer-involved shootings in {OIS_YEAR} "
                f"(WaPo dataset; county population unknown)"
            )
        if not wapo_ag:
            notes += "; agency absent from WaPo (no historical fatal OIS recorded)"
        upsert_metric(
            supabase,
            oid,
            name,
            metric_key="officer_involved_shootings",
            metric_label="Officer-Involved Shootings",
            metric_value=str(count),
            metric_unit="incidents",
            benchmark_value=("1.0" if rate is not None else None),
            benchmark_label=("<=1 per 100K = good" if rate is not None else None),
            performance_rating=rating,
            year=OIS_YEAR,
            source="Washington Post fatal-police-shootings dataset",
            source_url="https://github.com/washingtonpost/data-police-shootings",
            notes=notes,
        )
        filled += 1
        sources_hit.append("WaPo OIS")
    except Exception as e:
        LOG.warning("OIS fetch failed for %s: %s", name, e)
        log_failure(supabase, oid, name, f"WaPo OIS fetch error: {e}")

    # 3-5. Skipped metrics — write a no_data row and a single scrape_failure each
    for metric_key, metric_label, unit, reason in SKIP_METRIC_DEFS:
        try:
            upsert_metric(
                supabase,
                oid,
                name,
                metric_key=metric_key,
                metric_label=metric_label,
                metric_value="No public data",
                metric_unit=unit,
                performance_rating="no_data",
                year=SKIPPED_METRICS_YEAR,
                source="Manual research required",
                notes=reason[:300],
            )
        except Exception as e:
            LOG.warning("upsert no_data for %s/%s failed: %s", name, metric_key, e)
        log_failure(supabase, oid, name, f"{metric_key}: {reason}")

    return filled, sources_hit


# --- Driver ----------------------------------------------------------------


def fetch_sheriffs(supabase) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, level, district, party")
            .eq("state", "FL")
            .ilike("title", "%Sheriff%")
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--ids", type=str, default=None, help="Comma-separated official_ids")
    args = parser.parse_args()

    if not (DATA_GOV_KEY and SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing required env vars (DATA_GOV_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY)")
        return 1
    if not (os.path.exists(WAPO_AGENCIES) and os.path.exists(WAPO_SHOOTINGS)):
        LOG.error(
            "Missing local WaPo data files. Download from "
            "https://github.com/washingtonpost/data-police-shootings into data/wapo/"
        )
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    # Pre-load caches
    fetch_fl_county_populations()
    load_wapo_data()

    sheriffs = fetch_sheriffs(supabase)
    if args.ids:
        wanted = {int(x) for x in args.ids.split(",")}
        sheriffs = [s for s in sheriffs if s["id"] in wanted]
    if args.limit:
        sheriffs = sheriffs[: args.limit]

    LOG.info("Processing %d FL sheriffs", len(sheriffs))

    total_filled = 0
    failures = 0
    processed = 0
    for off in sheriffs:
        processed += 1
        try:
            n, sources = process_sheriff(supabase, off)
            total_filled += n
            LOG.info(
                "[%d/%d] %s: filled %d real metrics via %s",
                processed,
                len(sheriffs),
                off["name"],
                n,
                ", ".join(sources) if sources else "no sources",
            )
        except Exception as e:
            failures += 1
            LOG.exception("Failed %s: %s", off.get("name"), e)
            log_failure(supabase, off["id"], off.get("name", ""), f"Unhandled exception: {e}")

        if processed % 10 == 0:
            LOG.info(
                "Progress: %d/%d | %d real metrics filled | %d failures",
                processed,
                len(sheriffs),
                total_filled,
                failures,
            )

    LOG.info(
        "DONE: %d sheriffs processed | %d real metrics upserted | %d failures",
        processed,
        total_filled,
        failures,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
