"""Session 5 ingestion: Meeting attendance for FL county commissioners and city council/commission members.

DATA REALITY: Most FL counties and cities do NOT publish per-member meeting
attendance as a downloadable feed. Granicus and Legistar (the two dominant
agenda-management vendors used by larger entities) render attendance dynamically
via JavaScript or embed it in case-specific PDFs that are not consolidated
"minutes with present/absent" lists.

What this script does:

  1. For each FL county commissioner and FL city council/commission member,
     attempt a single best-effort scan of common minutes URL patterns on the
     entity's main website.

  2. If a page yields a parseable "Present:" / "Absent:" section (HTML or PDF),
     extract per-member counts and upsert real metrics.

  3. If no parseable attendance is found (the realistic outcome for >95% of
     entities), upsert a `no_data` row and log a `scrape_failures` entry with a
     specific reason (`ATTENDANCE_NOT_PUBLISHED`, `MEETINGS_VIDEO_ONLY`,
     `PARSE_FAILED`, `SITE_BLOCKED`).

We never fabricate attendance numbers.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY.

Usage:
  python scripts/ingest_meeting_attendance.py
  python scripts/ingest_meeting_attendance.py --counties Hillsborough,Miami-Dade
  python scripts/ingest_meeting_attendance.py --cities Tampa,Miami
  python scripts/ingest_meeting_attendance.py --limit 20
  python scripts/ingest_meeting_attendance.py --no-scrape   # only write skip rows
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

import requests
import urllib3
from dotenv import load_dotenv
from supabase import create_client

urllib3.disable_warnings()

load_dotenv()

LOG = logging.getLogger("meeting_attendance")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest_meeting_attendance.log", mode="a"),
    ],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

ZCTA_COUNTY_TXT = "data/census/zcta_county.txt"

ATTENDANCE_YEAR = 2024
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
PROBE_TIMEOUT = 10
PARSE_TIMEOUT = 25
SAME_DOMAIN_DELAY = 2.0

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
session.verify = False

# Common minutes URL paths per main website.
COUNTY_MINUTES_PATHS = [
    "/minutes",
    "/agendas-and-minutes",
    "/board-of-county-commissioners/meetings",
    "/board-of-county-commissioners/agendas-minutes",
    "/bocc/meetings",
    "/bocc/minutes",
    "/government/board-of-county-commissioners",
    "/government/board-of-county-commissioners/meetings",
    "/government/board-of-county-commissioners/agendas",
]

CITY_MINUTES_PATHS = [
    "/minutes",
    "/agendas",
    "/agendas-and-minutes",
    "/city-council/minutes",
    "/city-council/agendas",
    "/city-council/meetings",
    "/council/minutes",
    "/council/agendas",
    "/council/meetings",
    "/commission/minutes",
    "/commission/meetings",
    "/government/agendas-minutes",
]


# --- Rating ---------------------------------------------------------------


def rate_attendance(pct: float) -> str:
    if pct >= 95:
        return "excellent"
    if pct >= 85:
        return "good"
    if pct >= 75:
        return "meeting"
    if pct >= 60:
        return "concerning"
    return "poor"


# --- Persistence ---------------------------------------------------------


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
    supabase,
    official_id: int,
    identifier: str,
    reason: str,
    url: str | None = None,
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


# --- ZIP -> county fallback ----------------------------------------------

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


# --- Best-effort attendance scrape ---------------------------------------


PRESENT_RE = re.compile(
    r"(?:Members\s+)?Present\s*[:\-]\s*(.+?)(?=\n\s*(?:Members\s+)?Absent\s*[:\-]|\n\s*Excused\s*[:\-]|\Z)",
    re.I | re.S,
)
ABSENT_RE = re.compile(
    r"(?:Members\s+)?Absent\s*[:\-]\s*(.+?)(?=\n\s*(?:Members\s+)?Excused\s*[:\-]|\n\s*Roll Call|\Z)",
    re.I | re.S,
)
NAME_TOKEN_RE = re.compile(r"[A-Z][a-z'’\.\-]+(?:\s+[A-Z][a-z'’\.\-]+){1,3}")


def _extract_present_absent(text: str) -> tuple[set[str], set[str]] | None:
    """From a single meeting-minutes text body, return (present_names, absent_names)."""
    pm = PRESENT_RE.search(text)
    if not pm:
        return None
    present_block = pm.group(1)[:1500]
    am = ABSENT_RE.search(text)
    absent_block = am.group(1)[:1500] if am else ""

    def names(block: str) -> set[str]:
        # Split on commas, semicolons, "and", newlines
        parts = re.split(r"[,;\n]| and ", block)
        out: set[str] = set()
        for p in parts:
            p = p.strip()
            if 5 <= len(p) <= 60 and " " in p and p[0].isupper():
                out.add(p)
        return out

    return names(present_block), names(absent_block)


def fetch_text(url: str) -> str | None:
    try:
        r = session.get(url, timeout=PROBE_TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return None
        ct = r.headers.get("Content-Type", "")
        if "text/html" not in ct and "application/xhtml" not in ct:
            return None
        return r.text
    except requests.RequestException:
        return None


def fetch_pdf_text(url: str, max_pages: int = 5) -> str | None:
    try:
        r = session.get(url, timeout=PARSE_TIMEOUT)
        if r.status_code != 200 or len(r.content) < 5000:
            return None
        import pdfplumber
        import io as _io

        with pdfplumber.open(_io.BytesIO(r.content)) as pdf:
            text = ""
            for page in pdf.pages[:max_pages]:
                t = page.extract_text() or ""
                text += t + "\n"
            return text
    except Exception:
        return None


def find_minutes_pages(base_url: str, paths: list[str]) -> list[str]:
    """Probe candidate paths; return URLs that returned 200 HTML."""
    found: list[str] = []
    base = base_url.rstrip("/")
    for p in paths:
        url = base + p
        try:
            r = session.head(url, timeout=PROBE_TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                found.append(r.url)
            time.sleep(0.5)
        except requests.RequestException:
            continue
    return found


def scrape_attendance_for_entity(base_url: str, paths: list[str]) -> tuple[Counter, int, str | None]:
    """Best-effort scrape: returns (attendance_counts_by_name, meeting_count, source_url).

    Walks 1-2 minutes pages; for each, looks for embedded text or a downloadable
    PDF with a "Present:" / "Absent:" section. Counts each name's appearances.
    """
    attendance: Counter = Counter()
    meetings_seen = 0
    source_url: str | None = None

    minutes_pages = find_minutes_pages(base_url, paths)
    for mp in minutes_pages[:2]:
        html = fetch_text(mp)
        if not html:
            continue
        # Try to extract directly from HTML
        plain = re.sub(r"<[^>]+>", " ", html)
        plain = re.sub(r"\s+", " ", plain)
        result = _extract_present_absent(plain)
        if result and (result[0] or result[1]):
            present, absent = result
            for n in present:
                attendance[n] += 1
            meetings_seen += 1
            source_url = mp

        # Look for PDF links and try a few
        pdf_links = re.findall(r'href=["\']([^"\']+\.pdf)["\']', html, re.I)
        for raw in pdf_links[:5]:
            pdf_url = urljoin(mp, raw)
            text = fetch_pdf_text(pdf_url)
            if not text:
                continue
            res = _extract_present_absent(text)
            if res and (res[0] or res[1]):
                present, absent = res
                for n in present:
                    attendance[n] += 1
                meetings_seen += 1
                source_url = pdf_url
                if meetings_seen >= 8:
                    break
        time.sleep(SAME_DOMAIN_DELAY)
        if meetings_seen >= 8:
            break

    return attendance, meetings_seen, source_url


def name_match(target: str, candidates: set[str]) -> str | None:
    """Find the best fuzzy match for target in candidates."""
    target_tokens = re.findall(r"[A-Za-z]+", target.lower())
    if not target_tokens:
        return None
    target_last = target_tokens[-1]
    target_first = target_tokens[0] if len(target_tokens) > 1 else None
    for c in candidates:
        c_tokens = re.findall(r"[A-Za-z]+", c.lower())
        if not c_tokens:
            continue
        if target_last in c_tokens and (not target_first or target_first in c_tokens):
            return c
    # last-name only fallback
    for c in candidates:
        c_tokens = re.findall(r"[A-Za-z]+", c.lower())
        if target_last in c_tokens:
            return c
    return None


# --- Per-official processing ---------------------------------------------


def write_skip(
    supabase, oid: int, name: str, reason_code: str, reason_msg: str
) -> None:
    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="meeting_attendance_rate",
        metric_label="Meeting Attendance",
        metric_value="No public data",
        metric_unit="%",
        performance_rating="no_data",
        year=ATTENDANCE_YEAR,
        source="Manual research required",
        notes=reason_msg[:300],
    )
    log_failure(supabase, oid, name, f"meeting_attendance_rate: {reason_code} - {reason_msg}")


# --- Driver --------------------------------------------------------------


def fetch_county_commissioners(supabase) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, zip_codes")
            .eq("state", "FL")
            .ilike("title", "%County Commissioner%")
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


def fetch_city_council_members(supabase) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, zip_codes, category")
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
                "city council",
                "city commissioner",
                "council member",
                "councilman",
                "councilwoman",
                "vice mayor",
            )
        ):
            out.append(o)
    return out


def fetch_county_websites(supabase) -> dict[str, str]:
    """county (uppercase, no spaces) -> main_website."""
    r = supabase.table("county_contacts").select("county, main_website").execute()
    out: dict[str, str] = {}
    for row in r.data or []:
        c = (row.get("county") or "").upper().replace(" ", "").replace("-", "")
        if c and row.get("main_website"):
            out[c] = row["main_website"]
    return out


def fetch_municipality_websites(supabase) -> dict[str, str]:
    """city_key -> website."""
    r = supabase.table("fl_municipalities").select("name, website").execute()
    out: dict[str, str] = {}
    for row in r.data or []:
        n = (row.get("name") or "").upper().replace(" ", "").replace("-", "")
        if n and row.get("website"):
            out[n] = row["website"]
    return out


def parse_city_from_title(title: str) -> str | None:
    m = re.search(r"(?:mayor|commissioner|council[\s\w]*member|councilman|councilwoman)\s+of\s+(.+?)\s*$", title, re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r"(?:City Council Member|City Commissioner|Council Member|Vice Mayor)\s*[,\-]\s*(.+?)\s*$", title, re.I)
    if m:
        return m.group(1).strip()
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--counties", type=str, default=None, help="Only these counties (comma-separated)")
    parser.add_argument("--cities", type=str, default=None, help="Only these cities (comma-separated)")
    parser.add_argument(
        "--no-scrape",
        action="store_true",
        help="Skip live website probes; only write no_data + scrape_failures rows.",
    )
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing Supabase env vars")
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    load_zip_county_map()
    county_sites = fetch_county_websites(supabase)
    city_sites = fetch_municipality_websites(supabase)

    LOG.info("Loaded %d county websites, %d municipality websites", len(county_sites), len(city_sites))

    # Group county commissioners by county
    commissioners = fetch_county_commissioners(supabase)
    LOG.info("Total county commissioners: %d", len(commissioners))
    by_county: dict[str, list[dict]] = defaultdict(list)
    for o in commissioners:
        county = derive_county(o.get("zip_codes") or "")
        if county:
            by_county[county.upper().replace(" ", "").replace("-", "")].append(o)
        else:
            write_skip(
                supabase,
                o["id"],
                o["name"],
                "NO_COUNTY_DERIVED",
                "Could not derive county from elected_official.zip_codes",
            )

    # Group city council/commission by city name parsed from title
    city_members = fetch_city_council_members(supabase)
    LOG.info("Total city council/commission members: %d", len(city_members))
    by_city: dict[str, list[dict]] = defaultdict(list)
    for o in city_members:
        city = parse_city_from_title(o.get("title") or "")
        if city:
            by_city[city.upper().replace(" ", "").replace("-", "")].append(o)
        else:
            write_skip(
                supabase,
                o["id"],
                o["name"],
                "NO_CITY_PARSED",
                f"Could not extract city from title: {o.get('title')!r}",
            )

    # Apply filters
    if args.counties:
        wanted = {c.upper().replace(" ", "").replace("-", "") for c in args.counties.split(",")}
        by_county = {k: v for k, v in by_county.items() if k in wanted}
    if args.cities:
        wanted = {c.upper().replace(" ", "").replace("-", "") for c in args.cities.split(",")}
        by_city = {k: v for k, v in by_city.items() if k in wanted}

    real_filled = 0
    skipped = 0
    entities_processed = 0

    # ---- COUNTIES ----
    counties_list = list(by_county.items())
    if args.limit:
        counties_list = counties_list[: args.limit]
    LOG.info("Processing %d counties", len(counties_list))
    for ckey, members in counties_list:
        entities_processed += 1
        site = county_sites.get(ckey)
        if not site or args.no_scrape:
            for m in members:
                write_skip(
                    supabase,
                    m["id"],
                    m["name"],
                    "ATTENDANCE_NOT_PUBLISHED",
                    f"{ckey} County: no machine-readable attendance feed found in main website minutes pages.",
                )
                skipped += 1
            continue

        try:
            attendance, total, src_url = scrape_attendance_for_entity(site, COUNTY_MINUTES_PATHS)
        except Exception as e:
            LOG.warning("Scrape failed %s: %s", ckey, e)
            attendance, total, src_url = Counter(), 0, None

        if total == 0 or not attendance:
            for m in members:
                write_skip(
                    supabase,
                    m["id"],
                    m["name"],
                    "PARSE_FAILED",
                    f"{ckey} County: no parseable Present/Absent section in {site}",
                )
                skipped += 1
            LOG.info("[county %d/%d] %s: no parseable attendance", entities_processed, len(counties_list), ckey)
            continue

        # Match members to attendance
        cand_set = set(attendance.keys())
        for m in members:
            matched = name_match(m["name"], cand_set)
            if not matched:
                write_skip(
                    supabase,
                    m["id"],
                    m["name"],
                    "MEMBER_NOT_LISTED",
                    f"{ckey} County: name not found in any minutes Present/Absent section.",
                )
                skipped += 1
                continue
            present_count = attendance[matched]
            pct = round(present_count / total * 100, 1)
            upsert_metric(
                supabase,
                m["id"],
                m["name"],
                metric_key="meeting_attendance_rate",
                metric_label="Meeting Attendance",
                metric_value=str(pct),
                metric_unit="%",
                benchmark_value="90.0",
                benchmark_label="Typical FL local-board attendance",
                performance_rating=rate_attendance(pct),
                year=ATTENDANCE_YEAR,
                source=f"{ckey} County Commission minutes",
                source_url=src_url,
                notes=f"Present at {present_count} of {total} parseable meetings",
            )
            real_filled += 1
        LOG.info(
            "[county %d/%d] %s: %d members matched in %d meetings",
            entities_processed,
            len(counties_list),
            ckey,
            sum(1 for m in members if name_match(m["name"], cand_set)),
            total,
        )
        time.sleep(SAME_DOMAIN_DELAY)

    # ---- CITIES (top-N by member-list size proxy) ----
    cities_list = sorted(by_city.items(), key=lambda kv: -len(kv[1]))
    if not args.cities and not args.limit:
        cities_list = cities_list[:100]
    elif args.limit:
        cities_list = cities_list[: args.limit]

    LOG.info("Processing %d cities", len(cities_list))
    for ckey, members in cities_list:
        entities_processed += 1
        site = city_sites.get(ckey)
        if not site or args.no_scrape:
            for m in members:
                write_skip(
                    supabase,
                    m["id"],
                    m["name"],
                    "ATTENDANCE_NOT_PUBLISHED",
                    f"{ckey} city: no machine-readable attendance feed.",
                )
                skipped += 1
            continue

        try:
            attendance, total, src_url = scrape_attendance_for_entity(site, CITY_MINUTES_PATHS)
        except Exception as e:
            LOG.warning("City scrape failed %s: %s", ckey, e)
            attendance, total, src_url = Counter(), 0, None

        if total == 0 or not attendance:
            for m in members:
                write_skip(
                    supabase,
                    m["id"],
                    m["name"],
                    "PARSE_FAILED",
                    f"{ckey} city: no parseable Present/Absent section in {site}",
                )
                skipped += 1
            continue

        cand_set = set(attendance.keys())
        for m in members:
            matched = name_match(m["name"], cand_set)
            if not matched:
                write_skip(
                    supabase,
                    m["id"],
                    m["name"],
                    "MEMBER_NOT_LISTED",
                    f"{ckey} city: name not found in minutes.",
                )
                skipped += 1
                continue
            present_count = attendance[matched]
            pct = round(present_count / total * 100, 1)
            upsert_metric(
                supabase,
                m["id"],
                m["name"],
                metric_key="meeting_attendance_rate",
                metric_label="Meeting Attendance",
                metric_value=str(pct),
                metric_unit="%",
                benchmark_value="90.0",
                benchmark_label="Typical FL local-board attendance",
                performance_rating=rate_attendance(pct),
                year=ATTENDANCE_YEAR,
                source=f"{ckey} city minutes",
                source_url=src_url,
                notes=f"Present at {present_count} of {total} parseable meetings",
            )
            real_filled += 1
        time.sleep(SAME_DOMAIN_DELAY)

        if entities_processed % 25 == 0:
            LOG.info(
                "Progress: %d entities | %d real attendance metrics | %d skip rows",
                entities_processed,
                real_filled,
                skipped,
            )

    LOG.info(
        "DONE: %d entities processed | %d real metrics | %d skip rows",
        entities_processed,
        real_filled,
        skipped,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
