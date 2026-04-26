"""Session 3 ingestion: FL school board accountability metrics.

For each FL school board member (347 across 67 counties), upserts:
  district_grade        FLDOE 2023-24 District Grades XLSX
  graduation_rate       Same XLSX (column "Graduation Rate 2022-23")
  per_pupil_spending    FLDOE 2024-25 ESSA Per-pupil Expenditures (PDF, hardcoded)
  teacher_retention     SKIPPED — no public per-district file from FLDOE

County for each board member is derived from their ZIP codes via the Census
2020 ZCTA-county relationship file (data/census/zcta_county.txt).

Pre-reqs (run once):
  data/fldoe/district_grades_24.xlsx
  data/census/zcta_county.txt

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY

Usage:
  python scripts/ingest_school_board_metrics.py
  python scripts/ingest_school_board_metrics.py --counties Flagler,Volusia
  python scripts/ingest_school_board_metrics.py --ids 1679,1717
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

import openpyxl
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

LOG = logging.getLogger("school_board_metrics")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest_school_board_metrics.log", mode="a"),
    ],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

DISTRICT_GRADES_XLSX = "data/fldoe/district_grades_24.xlsx"
ZCTA_COUNTY_TXT = "data/census/zcta_county.txt"

GRADE_YEAR = 2024  # 2023-24 grades
GRAD_YEAR = 2023  # graduation rate is "2022-23" cohort
PPE_YEAR = 2025  # 2024-25 PPE
TEACHER_YEAR = 2024


# --- Per-pupil expenditure (extracted from FLDOE ESSA-2025-District-State.pdf) -

# Total Costs - State, Local and Federal Funds Per Pupil, 2024-25
PPE_BY_DISTRICT: dict[str, int] = {
    "ALACHUA": None,  # data not finalized in PDF
    "BAKER": 10275,
    "BAY": 10779,
    "BRADFORD": 11116,
    "BREVARD": 11548,
    "BROWARD": 11711,
    "CALHOUN": 11569,
    "CHARLOTTE": 13047,
    "CITRUS": 10868,
    "CLAY": 10535,
    "COLLIER": 14369,
    "COLUMBIA": 10007,
    "DADE": 12575,
    "DESOTO": 12158,
    "DIXIE": 10836,
    "DUVAL": 10524,
    "ESCAMBIA": 10768,
    "FLAGLER": 10451,
    "FRANKLIN": 15687,
    "GADSDEN": 14515,
    "GILCHRIST": 11772,
    "GLADES": 16306,
    "GULF": 13640,
    "HAMILTON": 13047,
    "HARDEE": 11024,
    "HENDRY": 8322,
    "HERNANDO": 10702,
    "HIGHLANDS": 11663,
    "HILLSBOROUGH": 10729,
    "HOLMES": 11020,
    "INDIAN RIVER": 11661,
    "JACKSON": 11675,
    "JEFFERSON": 18362,
    "LAFAYETTE": 13230,
    "LAKE": 10772,
    "LEE": 10860,
    "LEON": 10648,
    "LEVY": 11321,
    "LIBERTY": 11353,
    "MADISON": None,
    "MANATEE": 11354,
    "MARION": 10860,
    "MARTIN": 12701,
    "MONROE": 17752,
    "NASSAU": 11793,
    "OKALOOSA": 10293,
    "OKEECHOBEE": 12260,
    "ORANGE": 11241,
    "OSCEOLA": 10133,
    "PALM BEACH": 14408,
    "PASCO": 10200,
    "PINELLAS": 12472,
    "POLK": 10415,
    "PUTNAM": 11352,
    "ST. JOHNS": 9534,
    "ST. LUCIE": 10434,
    "SANTA ROSA": 9742,
    "SARASOTA": 14881,
    "SEMINOLE": 9957,
    "SUMTER": 14269,
    "SUWANNEE": 9983,
    "TAYLOR": 11593,
    "UNION": 11206,
    "VOLUSIA": 10408,
    "WAKULLA": 9807,
    "WALTON": 14468,
    "WASHINGTON": 11272,
}
PPE_STATE_AVERAGE = 11531

TEACHER_RETENTION_REASON = (
    "FLDOE does not publish per-district teacher retention as a downloadable file. "
    "Data exists internally but requires a public-records request to the district."
)


# --- Rating helpers --------------------------------------------------------


def rate_grade(g: str) -> str:
    return {
        "A": "excellent",
        "B": "good",
        "C": "meeting",
        "D": "concerning",
        "F": "poor",
    }.get((g or "").strip().upper(), "no_data")


def rate_grad(pct: float) -> str:
    if pct >= 92:
        return "excellent"
    if pct >= 88:
        return "good"
    if pct >= 84:
        return "meeting"
    if pct >= 80:
        return "concerning"
    return "poor"


def rate_ppe(amount: int) -> str:
    if amount > 12000:
        return "excellent"
    if amount >= 10000:
        return "good"
    if amount >= 9000:
        return "meeting"
    if amount >= 8000:
        return "concerning"
    return "poor"


# --- Source loaders --------------------------------------------------------


_district_grades_cache: dict[str, dict] | None = None


def load_district_grades() -> dict[str, dict]:
    """county_key (no spaces, lowercase) -> {grade, grad_rate, district_number, district_name}."""
    global _district_grades_cache
    if _district_grades_cache is not None:
        return _district_grades_cache
    out: dict[str, dict] = {}
    wb = openpyxl.load_workbook(DISTRICT_GRADES_XLSX, data_only=True)
    ws = wb["DG"]
    headers = None
    for ri, row in enumerate(ws.iter_rows(values_only=True)):
        if ri == 3:
            headers = list(row)
            continue
        if ri < 4:
            continue
        if not row or not row[0]:
            continue
        district_num = str(row[0]).strip()
        district_name = (row[1] or "").strip()
        if not district_name:
            continue
        # Column 12: Graduation Rate 2022-23, Column 18: Grade 2024
        grad = row[12]
        grade = (row[18] or "").strip()
        try:
            grad_pct = float(grad) if grad not in (None, "") else None
        except (TypeError, ValueError):
            grad_pct = None
        out[_county_key(district_name)] = {
            "district_number": district_num,
            "district_name": district_name,
            "grade": grade if grade else None,
            "grad_rate": grad_pct,
        }
    _district_grades_cache = out
    LOG.info("Loaded district grades for %d FL districts", len(out))
    return out


_zip_county_cache: dict[str, Counter] | None = None


def load_zip_county_map() -> dict[str, Counter]:
    """ZIP -> Counter({county_name: area_weight})."""
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
            # Strip " County" suffix for matching
            cname = re.sub(r"\s+County$", "", county_name).upper()
            out[zcta][cname] += area
    _zip_county_cache = out
    LOG.info("Loaded ZIP-county map: %d FL ZIPs", len(out))
    return out


def _county_key(s: str) -> str:
    """Normalize for matching: uppercase, no spaces, no punctuation."""
    return re.sub(r"[^A-Z]", "", s.upper())


def derive_county(zip_codes_str: str) -> str | None:
    """Pick the dominant county across a board member's ZIP list."""
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


# --- Per-board-member processing ----------------------------------------


SOURCE_GRADES = "FLDOE 2023-24 School Grades"
SOURCE_GRADES_URL = (
    "https://www.fldoe.org/accountability/accountability-reporting/school-grades/"
)
SOURCE_PPE = "FLDOE 2024-25 ESSA Per-pupil Expenditures"
SOURCE_PPE_URL = "https://www.fldoe.org/finance/fl-edu-finance-program-fefp/essa.stml"


def process_board_member(supabase, off: dict, county_override: str | None = None) -> tuple[int, list[str]]:
    name = off["name"]
    oid = off["id"]
    sources_hit: list[str] = []
    filled = 0

    county = county_override or derive_county(off.get("zip_codes") or "")
    if not county:
        log_failure(supabase, oid, name, f"Could not derive county from ZIPs: {(off.get('zip_codes') or '')[:50]}")
        return 0, []

    grades = load_district_grades()
    key = _county_key(county)
    dist = grades.get(key)
    if not dist:
        log_failure(supabase, oid, name, f"No FLDOE district grade row for county {county!r} (key {key!r})")
        return 0, []

    # district_grade
    if dist["grade"]:
        upsert_metric(
            supabase,
            oid,
            name,
            metric_key="district_grade",
            metric_label="District Grade",
            metric_value=dist["grade"],
            metric_unit="grade",
            performance_rating=rate_grade(dist["grade"]),
            year=GRADE_YEAR,
            source=SOURCE_GRADES,
            source_url=SOURCE_GRADES_URL,
            notes=f"FLDOE 2023-24 grade for {dist['district_name']} County School District",
        )
        filled += 1
        sources_hit.append("FLDOE grade")

    # graduation_rate
    if dist["grad_rate"] is not None:
        upsert_metric(
            supabase,
            oid,
            name,
            metric_key="graduation_rate",
            metric_label="Graduation Rate",
            metric_value=str(dist["grad_rate"]),
            metric_unit="%",
            benchmark_value="88.0",
            benchmark_label="FL state average ~88%",
            performance_rating=rate_grad(dist["grad_rate"]),
            year=GRAD_YEAR,
            source=SOURCE_GRADES,
            source_url=SOURCE_GRADES_URL,
            notes=f"4-year cohort graduation rate 2022-23 for {dist['district_name']} County",
        )
        filled += 1
        sources_hit.append("FLDOE grad")

    # per_pupil_spending — match on normalized key so MIAMI-DADE ↔ DADE etc.
    ppe = None
    dn_key = _county_key(dist["district_name"])
    for d_name, d_ppe in PPE_BY_DISTRICT.items():
        if _county_key(d_name) == dn_key:
            ppe = d_ppe
            break
    # Aliases for PDF naming differences
    if ppe is None and dn_key == "MIAMIDADE":
        ppe = PPE_BY_DISTRICT.get("DADE")
    if ppe is not None:
        upsert_metric(
            supabase,
            oid,
            name,
            metric_key="per_pupil_spending",
            metric_label="Per-Pupil Spending",
            metric_value=str(ppe),
            metric_unit="$/pupil",
            benchmark_value=str(PPE_STATE_AVERAGE),
            benchmark_label="FL state average",
            performance_rating=rate_ppe(ppe),
            year=PPE_YEAR,
            source=SOURCE_PPE,
            source_url=SOURCE_PPE_URL,
            notes=(
                f"Total state, local, and federal funds per pupil 2024-25 "
                f"for {dist['district_name']} County (FLDOE ESSA report)"
            ),
        )
        filled += 1
        sources_hit.append("FLDOE PPE")
    else:
        log_failure(
            supabase,
            oid,
            name,
            f"PPE not finalized for {dist['district_name']} in 2024-25 ESSA report",
        )

    # teacher_retention — no public per-district file
    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="teacher_retention",
        metric_label="Teacher Retention",
        metric_value="No public data",
        metric_unit="%",
        performance_rating="no_data",
        year=TEACHER_YEAR,
        source="Manual research required",
        notes=TEACHER_RETENTION_REASON[:300],
    )
    log_failure(supabase, oid, name, f"teacher_retention: {TEACHER_RETENTION_REASON}")

    return filled, sources_hit


# --- Driver ----------------------------------------------------------------


def fetch_board_members(supabase) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, district, zip_codes")
            .eq("state", "FL")
            .ilike("title", "%School Board%")
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
    parser.add_argument("--ids", type=str, default=None)
    parser.add_argument("--counties", type=str, default=None, help="Comma-separated county names")
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing Supabase env vars")
        return 1
    if not os.path.exists(DISTRICT_GRADES_XLSX):
        LOG.error("Missing %s — download from FLDOE first", DISTRICT_GRADES_XLSX)
        return 1
    if not os.path.exists(ZCTA_COUNTY_TXT):
        LOG.error("Missing %s — download from Census first", ZCTA_COUNTY_TXT)
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    load_district_grades()
    load_zip_county_map()

    members = fetch_board_members(supabase)
    if args.ids:
        wanted = {int(x) for x in args.ids.split(",")}
        members = [m for m in members if m["id"] in wanted]
    if args.counties:
        wanted = {_county_key(c) for c in args.counties.split(",")}
        members = [
            m
            for m in members
            if _county_key(derive_county(m.get("zip_codes") or "") or "") in wanted
        ]
    if args.limit:
        members = members[: args.limit]

    LOG.info("Processing %d FL school board members", len(members))

    total_filled = 0
    failures = 0
    processed = 0
    by_county: dict[str, int] = Counter()

    for off in members:
        processed += 1
        try:
            n, sources = process_board_member(supabase, off)
            total_filled += n
            county = derive_county(off.get("zip_codes") or "") or "Unknown"
            by_county[county] += 1
            LOG.info(
                "[%d/%d] %s (%s): filled %d real metrics via %s",
                processed,
                len(members),
                off["name"],
                county,
                n,
                ", ".join(sources) if sources else "no sources",
            )
        except Exception as e:
            failures += 1
            LOG.exception("Failed %s: %s", off.get("name"), e)
            log_failure(supabase, off["id"], off.get("name", ""), f"Unhandled: {e}")

        if processed % 25 == 0:
            LOG.info(
                "Progress: %d/%d | %d real metrics filled | %d failures",
                processed,
                len(members),
                total_filled,
                failures,
            )

    LOG.info(
        "DONE: %d board members processed | %d real metrics upserted | %d failures | %d distinct counties touched",
        processed,
        total_filled,
        failures,
        len(by_county),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
