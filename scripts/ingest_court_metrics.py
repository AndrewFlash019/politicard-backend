"""Session 6 ingestion: Court records for FL sheriffs (lawsuits) and judges (case throughput).

Sources used:
  CourtListener REST v4 (free, anonymous)
    /search/?type=r&court=flmd,flnd,flsd  — federal civil rights dockets
    /search/?type=o&court=fla              — FL Supreme Court opinions

What we fill (where data is publicly available at scale):
  lawsuits_settled        Per-sheriff count of federal civil rights lawsuits
                          (NOS 440-448, 550, 555) terminated in the last 5 years.
                          Saved with sample case names + docket URLs in `notes`.
  cases_disposed          FL Supreme Court Justices share the court's annual
                          opinion count (CourtListener doesn't expose
                          author_id for FL state opinions, so this is a
                          court-aggregate metric not per-justice).

What we skip (no public per-entity feed):
  reversal_rate           NOT_PUBLISHED_PER_JUDGE — FL appellate annual reports
                          don't expose per-judge reversal stats.
  cases_disposed (DCA / Circuit judges)
                          CourtListener combines all 6 DCAs into one court_id
                          (fladistctapp) and has no circuit court coverage;
                          per-DCA throughput requires scraping each DCA's
                          annual statistical PDF.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY.
Pre-reqs: data/census/zcta_county.txt (for county fallback if title parsing fails).

Usage:
  python scripts/ingest_court_metrics.py
  python scripts/ingest_court_metrics.py --only sheriffs
  python scripts/ingest_court_metrics.py --only judges
  python scripts/ingest_court_metrics.py --ids 427,471,476,1593
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, date

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

LOG = logging.getLogger("court_metrics")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest_court_metrics.log", mode="a"),
    ],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

CL_BASE = "https://www.courtlistener.com/api/rest/v4"
CL_DELAY = 2.0  # Anonymous tier is ~5/min; throttle to stay safe
USER_AGENT = "PolitiScore Civic Data Bot +https://politiscore.com"

LAWSUIT_WINDOW_YEARS = 5
LAWSUIT_YEAR = 2025  # year stamp for the metric (most-recent-window-end)
JUDGE_YEAR = 2024  # year stamp for cases_disposed

# Federal civil rights NOS codes (from PACER schema)
CIVIL_RIGHTS_NOS = ["440", "441", "442", "443", "445", "446", "448", "550", "555"]

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# --- Rating helpers --------------------------------------------------------


def rate_lawsuits(count: int) -> str:
    if count == 0:
        return "excellent"
    if count <= 3:
        return "good"
    if count <= 9:
        return "meeting"
    if count <= 20:
        return "concerning"
    return "poor"


def rate_cases_disposed(count: int) -> str:
    if count > 800:
        return "excellent"
    if count >= 500:
        return "good"
    if count >= 300:
        return "meeting"
    if count >= 150:
        return "concerning"
    return "poor"


# --- HTTP helper ----------------------------------------------------------


def http_get(url: str, params: dict | None = None, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = 2 ** attempt * 10
                LOG.warning("CourtListener 429, sleeping %ds", wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = 2 ** attempt * 5
                LOG.warning("CourtListener %d, retry in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            LOG.warning("HTTP error attempt %d: %s — %s", attempt + 1, url[:100], e)
            time.sleep(3)
    return None


# --- Sheriff lawsuit search ----------------------------------------------


def sheriff_search_terms(name: str, county: str) -> list[str]:
    """Build a list of search query terms for a sheriff."""
    last = name.split()[-1] if name else ""
    return [
        f'"{county} County Sheriff"',
        f'"Sheriff {last}"' if last else "",
        f'"{county} Sheriff"',
    ]


_NOS_RE = re.compile(r"\b(440|441|442|443|445|446|448|550|555)\b|civil rights|prison(?:er)? civil", re.I)


def _is_civil_rights(suit_nature: str | None) -> bool:
    if not suit_nature:
        return False
    return bool(_NOS_RE.search(suit_nature))


def search_sheriff_lawsuits(name: str, county: str) -> tuple[int, list[dict]]:
    """Return (count, sample_cases). One CourtListener call combines all
    sheriff search terms via OR; we then filter civil-rights cases client-side
    by inspecting suitNature. Cuts API calls from 27 to 1-2 per sheriff."""
    cutoff = date.today().replace(year=date.today().year - LAWSUIT_WINDOW_YEARS).isoformat()
    last = name.split()[-1] if name else ""
    or_terms = [f'"{county} County Sheriff"']
    if last:
        or_terms.append(f'"Sheriff {last}"')
    q = " OR ".join(or_terms)

    docket_ids: set[int] = set()
    sample: list[dict] = []
    page = 1
    while True:
        d = http_get(
            f"{CL_BASE}/search/",
            params={
                "type": "r",
                "q": q,
                "court": "flmd,flnd,flsd",
                "filed_after": cutoff,
                "page_size": 100,
                "page": page,
            },
        )
        time.sleep(CL_DELAY)
        if not d:
            break
        results = d.get("results", [])
        if not results:
            break
        for res in results:
            if not _is_civil_rights(res.get("suitNature")):
                continue
            did = res.get("docket_id")
            if did and did not in docket_ids:
                docket_ids.add(did)
                if len(sample) < 5:
                    sample.append(
                        {
                            "case": res.get("caseName"),
                            "filed": res.get("dateFiled"),
                            "nos": res.get("suitNature"),
                            "url": "https://www.courtlistener.com"
                            + (res.get("docket_absolute_url") or ""),
                        }
                    )
        if not d.get("next") or page >= 3:
            break
        page += 1

    return len(docket_ids), sample


def parse_county(title: str) -> str | None:
    m = re.match(r"Sheriff,\s*(.+?)\s*County", title)
    if m:
        return m.group(1).strip()
    m = re.match(r"^(.+?)\s+County\s+Sheriff", title)
    if m:
        return m.group(1).strip()
    return None


# --- Judge: court-wide opinion count -------------------------------------


# Pre-fetched once via CourtListener /search/?type=o&court=fla&filed_after=2024-01-01&filed_before=2024-12-31
# Hardcoded so we can fill the 7 SC Justices without re-hitting the throttled API.
_FLA_OPINION_COUNT_CACHE = {2024: 131}


def fetch_fla_opinion_count(year: int) -> int | None:
    """Count of FL Supreme Court opinions filed in `year`."""
    if year in _FLA_OPINION_COUNT_CACHE:
        return _FLA_OPINION_COUNT_CACHE[year]
    d = http_get(
        f"{CL_BASE}/search/",
        params={
            "type": "o",
            "court": "fla",
            "filed_after": f"{year}-01-01",
            "filed_before": f"{year}-12-31",
            "page_size": 1,
        },
    )
    time.sleep(CL_DELAY)
    if not d:
        return None
    _FLA_OPINION_COUNT_CACHE[year] = d.get("count")
    return _FLA_OPINION_COUNT_CACHE[year]


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


# --- Per-sheriff processing -----------------------------------------------


def process_sheriff(supabase, off: dict, skip_search: bool = False) -> int:
    name = off["name"]
    oid = off["id"]
    title = off.get("title") or ""
    county = parse_county(title)
    if not county:
        log_failure(supabase, oid, name, f"Could not parse county from sheriff title: {title!r}")
        return 0

    if skip_search:
        log_failure(
            supabase,
            oid,
            name,
            "lawsuits_settled: CourtListener anonymous-tier rate-limited (23-hour backoff). Re-run with API token.",
        )
        return 0

    try:
        count, sample = search_sheriff_lawsuits(name, county)
    except Exception as e:
        LOG.warning("CourtListener fetch failed for %s: %s", name, e)
        log_failure(supabase, oid, name, f"CourtListener search error: {e}")
        return 0

    sample_str = "; ".join(
        f"{s['case']} ({s['filed']})" for s in sample[:3]
    )
    notes = (
        f"{count} federal civil rights cases (NOS 440-448, 550, 555) "
        f"vs {county} County Sheriff's Office or Sheriff {name.split()[-1]} "
        f"in FLMD/FLND/FLSD filed since {LAWSUIT_YEAR - LAWSUIT_WINDOW_YEARS}. "
    )
    if sample_str:
        notes += f"Sample: {sample_str[:200]}"

    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="lawsuits_settled",
        metric_label="Civil Rights Lawsuits",
        metric_value=str(count),
        metric_unit="cases",
        benchmark_value="3.0",
        benchmark_label="Background level <=3 over 5yr",
        performance_rating=rate_lawsuits(count),
        year=LAWSUIT_YEAR,
        source="CourtListener (RECAP/PACER mirror)",
        source_url="https://www.courtlistener.com/recap/",
        notes=notes[:500],
    )
    return 1


# --- Per-judge processing ------------------------------------------------


SUPREME_COURT_TITLES = ("Justice, Florida Supreme Court", "Chief Justice, Florida Supreme Court")


def process_judge(supabase, off: dict) -> int:
    name = off["name"]
    oid = off["id"]
    title = off.get("title") or ""

    filled = 0

    if title in SUPREME_COURT_TITLES:
        # Court-aggregate cases_disposed via CourtListener
        court_count = fetch_fla_opinion_count(JUDGE_YEAR)
        if court_count:
            upsert_metric(
                supabase,
                oid,
                name,
                metric_key="cases_disposed",
                metric_label="Court Opinions Disposed",
                metric_value=str(court_count),
                metric_unit="opinions",
                performance_rating=rate_cases_disposed(court_count),
                year=JUDGE_YEAR,
                source="CourtListener (court-aggregate)",
                source_url="https://www.courtlistener.com/?type=o&court=fla",
                notes=(
                    f"FL Supreme Court issued {court_count} opinions in {JUDGE_YEAR}. "
                    f"CourtListener does not expose author_id for FL state opinions, "
                    f"so this is a court-aggregate metric attributed to all 7 justices."
                ),
            )
            filled += 1
        else:
            log_failure(
                supabase,
                oid,
                name,
                f"CourtListener returned no FL SC opinion count for {JUDGE_YEAR}",
            )
    else:
        # DCA Chief Judges and Circuit Chief Judges — no per-judge feed
        log_failure(
            supabase,
            oid,
            name,
            (
                "cases_disposed: NOT_PUBLISHED_PER_JUDGE for DCA / Circuit Chief Judges. "
                "CourtListener combines all 6 DCAs into one court_id (fladistctapp) and "
                "has no FL circuit court coverage; per-judge throughput requires "
                "scraping each court's annual statistical PDF."
            ),
        )
        upsert_metric(
            supabase,
            oid,
            name,
            metric_key="cases_disposed",
            metric_label="Court Opinions Disposed",
            metric_value="No public data",
            metric_unit="opinions",
            performance_rating="no_data",
            year=JUDGE_YEAR,
            source="Manual research required",
            notes=(
                "DCA / Circuit per-judge case counts are not aggregated in any "
                "machine-readable feed. Each court's annual statistical report "
                "would need PDF parsing."
            ),
        )

    # reversal_rate — universally not published per judge
    log_failure(
        supabase,
        oid,
        name,
        "reversal_rate: NOT_PUBLISHED_PER_JUDGE for any FL court level.",
    )
    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="reversal_rate",
        metric_label="Reversal Rate on Appeal",
        metric_value="No public data",
        metric_unit="%",
        performance_rating="no_data",
        year=JUDGE_YEAR,
        source="Manual research required",
        notes=(
            "Per-judge reversal rates are not published by any FL court "
            "(SC, DCA, or Circuit). Aggregate court reversal rates are only "
            "available in some annual reports."
        ),
    )

    return filled


# --- Driver --------------------------------------------------------------


def fetch_sheriffs(supabase) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, district")
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


def fetch_judges(supabase) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title")
            .eq("state", "FL")
            .or_("title.ilike.%Justice%,title.ilike.%Judge%")
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
    parser.add_argument("--only", choices=("sheriffs", "judges", "all"), default="all")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--ids", type=str, default=None)
    parser.add_argument(
        "--skip-search",
        action="store_true",
        help="Skip live CourtListener queries; only write scrape_failures for unprocessed sheriffs.",
    )
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing Supabase env vars")
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    sheriffs: list[dict] = []
    judges: list[dict] = []
    if args.only in ("sheriffs", "all"):
        sheriffs = fetch_sheriffs(supabase)
    if args.only in ("judges", "all"):
        judges = fetch_judges(supabase)

    if args.ids:
        wanted = {int(x) for x in args.ids.split(",")}
        sheriffs = [s for s in sheriffs if s["id"] in wanted]
        judges = [j for j in judges if j["id"] in wanted]
    if args.limit:
        sheriffs = sheriffs[: args.limit]
        judges = judges[: args.limit]

    LOG.info("Processing %d sheriffs and %d judges", len(sheriffs), len(judges))

    real_filled = 0
    failures = 0

    # Sheriffs
    skip = args.skip_search
    if skip:
        # Don't redo sheriffs that already have a non-stale 2025 lawsuit row.
        already = supabase.table("accountability_metrics").select("official_id").eq(
            "metric_key", "lawsuits_settled"
        ).eq("year", LAWSUIT_YEAR).eq("source", "CourtListener (RECAP/PACER mirror)").execute()
        done_ids = {r["official_id"] for r in (already.data or [])}
        LOG.info("--skip-search active; %d sheriffs already have CourtListener data", len(done_ids))
        sheriffs = [s for s in sheriffs if s["id"] not in done_ids]
    for i, off in enumerate(sheriffs, 1):
        try:
            n = process_sheriff(supabase, off, skip_search=skip)
            real_filled += n
            LOG.info("[sheriff %d/%d] %s: %d real metric%s", i, len(sheriffs), off["name"], n, "s" if n != 1 else "")
        except Exception as e:
            failures += 1
            LOG.exception("Failed sheriff %s: %s", off.get("name"), e)
        if i % 10 == 0:
            LOG.info("Sheriff progress: %d/%d | %d real metrics | %d failures", i, len(sheriffs), real_filled, failures)

    # Judges
    for i, off in enumerate(judges, 1):
        try:
            n = process_judge(supabase, off)
            real_filled += n
            LOG.info("[judge %d/%d] %s (%s): %d real metric%s", i, len(judges), off["name"], off.get("title"), n, "s" if n != 1 else "")
        except Exception as e:
            failures += 1
            LOG.exception("Failed judge %s: %s", off.get("name"), e)

    LOG.info(
        "DONE: %d sheriffs + %d judges processed | %d real metrics upserted | %d failures",
        len(sheriffs),
        len(judges),
        real_filled,
        failures,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
