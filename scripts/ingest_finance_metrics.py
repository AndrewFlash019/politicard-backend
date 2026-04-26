"""Session 4 ingestion: County/city financial accountability metrics for FL local officials.

Targets:
  - Mayors / Vice Mayors / City Council / City Commissioners (city-level)
  - County Commissioners (county-level)

What we fill (where data is publicly available at scale):
  population_change   Census ACS 5-year estimates 2018 vs 2023 for FL counties
                      and FL Census places (~370). Per-entity rate; fanned out
                      to every official tied to that entity.

What we skip (no public per-entity feed):
  budget_variance               EDR has actual expenditures only — no budgeted
                                column. Per-entity CAFR PDFs would be needed.
  bond_rating                   Moody's/S&P public ratings are paywalled at scale.
  tax_rate_change               FL DOR publishes only the current year as XLSX;
                                historical millage data is in 67 PDFs/year.
  capital_projects_on_time      Per-CIP report parsing required.
  capital_projects_on_budget    Same.
  public_records_response_time  Per-entity FOIA audit required.

For each skipped metric we upsert a no_data row plus a scrape_failures entry
documenting WHY the source isn't usable at scale.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (optional CENSUS_API_KEY).
Pre-reqs: data/census/zcta_county.txt (from Session 3).

Usage:
  python scripts/ingest_finance_metrics.py
  python scripts/ingest_finance_metrics.py --limit 20
  python scripts/ingest_finance_metrics.py --ids 446,2036,2535
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

LOG = logging.getLogger("finance_metrics")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest_finance_metrics.log", mode="a"),
    ],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
CENSUS_KEY = os.getenv("CENSUS_API_KEY")

ZCTA_COUNTY_TXT = "data/census/zcta_county.txt"

POP_BASE_YEAR = 2018  # 5-yr ACS centered on 2014-2018
POP_LATEST_YEAR = 2023  # 5-yr ACS centered on 2019-2023
POP_METRIC_YEAR = 2023  # year stamp for the metric


# --- Skip-with-reason scaffolding -----------------------------------------

CITY_SKIP_METRICS = [
    (
        "budget_variance",
        "Budget Variance",
        "%",
        "REQUIRES_PER_ENTITY_CAFR: FL EDR feeds actual expenditures only. Comparing actuals to adopted budgets requires parsing each city's annual CAFR PDF.",
    ),
    (
        "bond_rating",
        "Bond Rating",
        "rating",
        "NOT_PUBLISHED_AS_FEED: Moody's/S&P ratings are behind paywall at scale. Most small FL cities have no rated debt.",
    ),
    (
        "tax_rate_change",
        "Property Tax Rate Change",
        "mills",
        "NOT_PUBLISHED_AS_FEED: FL DOR publishes only the current year millage XLSX; historical millage data is fragmented across 67 per-county PDFs/year.",
    ),
    (
        "capital_projects_on_time",
        "Capital Projects On-Time",
        "%",
        "REQUIRES_PER_ENTITY_FOIA: City CIP reports are not standardized or aggregated.",
    ),
    (
        "capital_projects_on_budget",
        "Capital Projects On-Budget",
        "%",
        "REQUIRES_PER_ENTITY_FOIA: City CIP reports are not standardized or aggregated.",
    ),
    (
        "public_records_response_time",
        "Public Records Response Time",
        "days",
        "REQUIRES_PER_ENTITY_FOIA: No statewide audit of FL Sunshine Law response times by entity.",
    ),
]

COUNTY_SKIP_METRICS = [
    (
        "budget_variance",
        "Budget Variance",
        "%",
        "REQUIRES_PER_ENTITY_CAFR: FL EDR feeds actual expenditures only. Adopted budgets live in each county's CAFR PDF.",
    ),
    (
        "bond_rating",
        "Bond Rating",
        "rating",
        "NOT_PUBLISHED_AS_FEED: Moody's/S&P ratings are behind paywall at scale.",
    ),
    (
        "tax_rate_change",
        "Property Tax Rate Change",
        "mills",
        "NOT_PUBLISHED_AS_FEED: Historical county millage data is in per-county PDFs.",
    ),
    (
        "capital_projects_on_time",
        "Capital Projects On-Time",
        "%",
        "REQUIRES_PER_ENTITY_FOIA",
    ),
    (
        "capital_projects_on_budget",
        "Capital Projects On-Budget",
        "%",
        "REQUIRES_PER_ENTITY_FOIA",
    ),
    (
        "public_records_response_time",
        "Public Records Response Time",
        "days",
        "REQUIRES_PER_ENTITY_FOIA",
    ),
]


# --- Rating helpers --------------------------------------------------------


def rate_pop_change(pct: float) -> str:
    if pct > 5:
        return "excellent"
    if pct >= 1:
        return "good"
    if pct >= 0:
        return "meeting"
    if pct >= -2:
        return "concerning"
    return "poor"


# --- Census data loaders --------------------------------------------------


def _census_url(year: int, geo: str) -> str:
    base = f"https://api.census.gov/data/{year}/acs/acs5"
    sep = f"&key={CENSUS_KEY}" if CENSUS_KEY else ""
    if geo == "county":
        return f"{base}?get=NAME,B01003_001E&for=county:*&in=state:12{sep}"
    return f"{base}?get=NAME,B01003_001E&for=place:*&in=state:12{sep}"


_county_pop_cache: dict[int, dict[str, int]] = {}
_place_pop_cache: dict[int, dict[str, int]] = {}


def _county_key(s: str) -> str:
    return re.sub(r"[^A-Z]", "", s.upper())


def _place_key(s: str) -> str:
    """Normalize a city/town name. Strip 'City of ', 'Town of ', and place-type suffixes."""
    s = re.sub(r"^(?:city|town|village)\s+of\s+", "", s, flags=re.I)
    s = re.sub(r"\s+(?:city|town|village|cdp)$", "", s, flags=re.I)
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def fetch_county_pops(year: int) -> dict[str, int]:
    """county_key -> population for FL counties for the given ACS 5-yr year."""
    if year in _county_pop_cache:
        return _county_pop_cache[year]
    r = requests.get(_census_url(year, "county"), timeout=60)
    r.raise_for_status()
    data = r.json()
    out: dict[str, int] = {}
    for row in data[1:]:
        name = row[0]  # "Alachua County, Florida"
        try:
            pop = int(row[1])
        except (TypeError, ValueError):
            continue
        m = re.match(r"^(.+?)\s+County,\s+Florida$", name)
        if m:
            out[_county_key(m.group(1))] = pop
    _county_pop_cache[year] = out
    LOG.info("Cached %d FL county populations for ACS %d", len(out), year)
    return out


def fetch_place_pops(year: int) -> dict[str, int]:
    """place_key -> population for FL Census places for the given ACS 5-yr year."""
    if year in _place_pop_cache:
        return _place_pop_cache[year]
    r = requests.get(_census_url(year, "place"), timeout=60)
    r.raise_for_status()
    data = r.json()
    out: dict[str, int] = {}
    for row in data[1:]:
        name = row[0]  # "Alachua city, Florida"
        try:
            pop = int(row[1])
        except (TypeError, ValueError):
            continue
        m = re.match(r"^(.+?),\s+Florida$", name)
        if not m:
            continue
        out[_place_key(m.group(1))] = pop
    _place_pop_cache[year] = out
    LOG.info("Cached %d FL place populations for ACS %d", len(out), year)
    return out


# --- ZIP -> county fallback (for county commissioners) -------------------

_zip_county_cache: dict[str, Counter] | None = None


def load_zip_county_map() -> dict[str, Counter]:
    global _zip_county_cache
    if _zip_county_cache is not None:
        return _zip_county_cache
    out: dict[str, Counter] = defaultdict(Counter)
    with open(ZCTA_COUNTY_TXT, "r", encoding="utf-8") as f:
        rdr = csv.reader(f, delimiter="|")
        next(rdr)
        for row in rdr:
            zcta = row[1]
            county_geo = row[9]
            county_name = row[10]
            if not zcta or not county_geo.startswith("12"):
                continue
            try:
                area = int(row[16])
            except (ValueError, IndexError):
                area = 1
            cname = re.sub(r"\s+County$", "", county_name).upper()
            out[zcta][cname] += area
    _zip_county_cache = out
    LOG.info("Loaded ZIP-county map: %d FL ZIPs", len(out))
    return out


def derive_county(zip_codes_str: str) -> str | None:
    if not zip_codes_str:
        return None
    zip_map = load_zip_county_map()
    tally: Counter = Counter()
    for z in zip_codes_str.split(","):
        z = z.strip()
        if z in zip_map:
            for county, weight in zip_map[z].items():
                tally[county] += weight
    if not tally:
        return None
    return tally.most_common(1)[0][0]


# --- Title parsing --------------------------------------------------------


# Sentinel: official is a county commissioner (no specific city)
SCOPE_COUNTY = "__county__"


def classify_official(off: dict) -> tuple[str, str | None, str | None]:
    """Return (role_label, scope, entity_name) — scope is 'city' or 'county'."""
    title = (off.get("title") or "")
    tl = title.lower()
    category = (off.get("category") or "")

    if "county commissioner" in tl:
        county = derive_county(off.get("zip_codes") or "")
        return ("county_commissioner", "county", county)

    # City-scoped roles. Extract city from title.
    city = None
    m = re.search(r"(?:mayor|chair|vice\s*mayor|vice\s*chair)\s+of\s+(.+?)\s*$", title, re.I)
    if m:
        city = m.group(1)
    else:
        # "City Council Member, Orange Park" / "City Commissioner, Atlantic Beach"
        m = re.search(
            r"(?:city\s+council[\s\w]*member|councilman|councilwoman|city\s+commissioner|commissioner|"
            r"council\s+member|vice\s*chair|chair)\s*[,\-]\s*(.+?)\s*$",
            title,
            re.I,
        )
        if m:
            city = m.group(1)
        else:
            # Last-ditch: "<City> City Commission" / "<City> Commissioner"
            m = re.match(
                r"^(.+?)\s+(?:city\s+(?:council|commission|commissioner)|commissioner)\b",
                title,
                re.I,
            )
            if m:
                city = m.group(1)

    if city:
        # Strip "District N" / "Seat N" / "Ward N" trailing
        city = re.sub(
            r"[\s,;\-]*(district|seat|ward|group|at[\s\-]?large|position)\s*\d*[A-Za-z]?\s*$",
            "",
            city,
            flags=re.I,
        ).strip()
        # Strip parenthesized notes like "(Chair)"
        city = re.sub(r"\s*\([^)]*\)\s*$", "", city).strip()
    if "mayor" in tl and "vice" in tl:
        role = "vice_mayor"
    elif "mayor" in tl:
        role = "mayor"
    elif "vice" in tl:
        role = "vice_chair"
    elif "council" in tl:
        role = "city_council"
    elif "commissioner" in tl:
        role = "city_commissioner"
    else:
        role = "other"

    if city:
        return (role, "city", city)
    if "City Government" in category:
        return (role, "city", None)
    return (role, None, None)


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


def log_failure(
    supabase, official_id: int, identifier: str, reason: str, url: str | None = None
) -> None:
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


# --- Per-official processing ----------------------------------------------


def process_official(supabase, off: dict) -> tuple[int, str]:
    """Return (n_real_metrics_filled, status_label)."""
    name = off["name"]
    oid = off["id"]
    role, scope, entity = classify_official(off)

    if not scope:
        log_failure(supabase, oid, name, f"Could not classify scope from title: {off.get('title')!r}")
        return 0, "no_scope"

    # population_change
    pop_change = None
    pop_2018 = pop_2023 = None
    entity_label = entity
    if scope == "county":
        if not entity:
            log_failure(supabase, oid, name, "No county derived from ZIPs for county commissioner")
            _write_skip_rows(supabase, oid, name, COUNTY_SKIP_METRICS)
            return 0, "no_county"
        ck = _county_key(entity)
        pop_2018 = fetch_county_pops(POP_BASE_YEAR).get(ck)
        pop_2023 = fetch_county_pops(POP_LATEST_YEAR).get(ck)
        entity_label = f"{entity} County"
    else:  # city
        if not entity:
            log_failure(supabase, oid, name, f"No city extracted from title: {off.get('title')!r}")
            _write_skip_rows(supabase, oid, name, CITY_SKIP_METRICS)
            return 0, "no_city"
        pk = _place_key(entity)
        pop_2018 = fetch_place_pops(POP_BASE_YEAR).get(pk)
        pop_2023 = fetch_place_pops(POP_LATEST_YEAR).get(pk)
        entity_label = entity

    real_filled = 0
    if pop_2018 and pop_2023:
        pop_change = round((pop_2023 - pop_2018) / pop_2018 * 100, 2)
        upsert_metric(
            supabase,
            oid,
            name,
            metric_key="population_change",
            metric_label="Population Change",
            metric_value=str(pop_change),
            metric_unit="%",
            benchmark_value="2.0",
            benchmark_label="FL state growth ~2%/yr",
            performance_rating=rate_pop_change(pop_change),
            year=POP_METRIC_YEAR,
            source="US Census ACS 5-yr estimates",
            source_url="https://api.census.gov/data/2023/acs/acs5",
            notes=(
                f"{entity_label}: {pop_2018:,} (ACS {POP_BASE_YEAR}) -> "
                f"{pop_2023:,} (ACS {POP_LATEST_YEAR})"
            ),
        )
        real_filled += 1
    else:
        log_failure(
            supabase,
            oid,
            name,
            f"No Census ACS match for {scope}={entity!r} (key {(_county_key if scope=='county' else _place_key)(entity)!r})",
        )

    # Skip-with-reason rows for the other 5–6 spec metrics
    skip_set = COUNTY_SKIP_METRICS if scope == "county" else CITY_SKIP_METRICS
    _write_skip_rows(supabase, oid, name, skip_set)

    return real_filled, "ok"


def _write_skip_rows(
    supabase, oid: int, name: str, skips: list[tuple[str, str, str, str]]
) -> None:
    for metric_key, metric_label, unit, reason in skips:
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
                year=POP_METRIC_YEAR,
                source="Manual research required",
                notes=reason[:300],
            )
        except Exception as e:
            LOG.warning("upsert no_data %s/%s failed: %s", name, metric_key, e)
        log_failure(supabase, oid, name, f"{metric_key}: {reason}")


# --- Driver --------------------------------------------------------------


def fetch_target_officials(supabase) -> list[dict]:
    """Pull all officials whose title matches our finance-applicable roles."""
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, district, zip_codes, category")
            .eq("state", "FL")
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000

    out: list[dict] = []
    for o in rows:
        t = (o.get("title") or "").lower()
        if any(
            k in t
            for k in (
                "mayor",
                "vice mayor",
                "city council",
                "city commissioner",
                "council member",
                "councilman",
                "councilwoman",
                "county commissioner",
            )
        ):
            out.append(o)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--ids", type=str, default=None)
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing Supabase env vars")
        return 1
    if not os.path.exists(ZCTA_COUNTY_TXT):
        LOG.error("Missing %s — required for county lookup", ZCTA_COUNTY_TXT)
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    fetch_county_pops(POP_BASE_YEAR)
    fetch_county_pops(POP_LATEST_YEAR)
    fetch_place_pops(POP_BASE_YEAR)
    fetch_place_pops(POP_LATEST_YEAR)
    load_zip_county_map()

    officials = fetch_target_officials(supabase)
    if args.ids:
        wanted = {int(x) for x in args.ids.split(",")}
        officials = [o for o in officials if o["id"] in wanted]
    if args.limit:
        officials = officials[: args.limit]

    LOG.info("Processing %d FL local officials (mayor/council/commission)", len(officials))

    total_real = 0
    by_status: Counter = Counter()
    for i, off in enumerate(officials, 1):
        try:
            n, status = process_official(supabase, off)
            total_real += n
            by_status[status] += 1
        except Exception as e:
            LOG.exception("Failed %s: %s", off.get("name"), e)
            log_failure(supabase, off["id"], off.get("name", ""), f"Unhandled: {e}")
            by_status["error"] += 1
        if i % 50 == 0:
            LOG.info(
                "Progress: %d/%d | %d real population_change filled | status %s",
                i,
                len(officials),
                total_real,
                dict(by_status),
            )

    LOG.info(
        "DONE: %d officials processed | %d real population_change metrics | status %s",
        len(officials),
        total_real,
        dict(by_status),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
