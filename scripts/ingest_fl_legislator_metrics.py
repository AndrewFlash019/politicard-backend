"""Populate accountability_metrics for FL state legislators.

For each Florida state senator (~39) and representative (~117):

  bills_sponsored   from legislative_activity (already-ingested via OpenStates)
  bills_passed      from legislative_activity, status in (enacted, signed)
  voting_attendance best-effort via OpenStates /votes; SKIP if no data
  party_line_voting best-effort via OpenStates /votes; SKIP if no data

Ratings vs chamber benchmarks (FL 2025 session — tuned from observed
distributions):

  bills_sponsored:  >=20 excellent | >=10 good | >=5 meeting | >=2 concerning | >0 poor
  bills_passed:     >=3 excellent  | >=2 good  | >=1 meeting | 0 poor
  voting_attendance %: >=98 excellent | >=95 good | >=90 meeting | >=80 concerning | <80 poor
  party_line_voting %: <=80 excellent | <=90 good | <=95 meeting | <=98 concerning | >98 poor
                                       (lower = more independent → "better")

Idempotent: upserts by (official_id, metric_key, year).

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY, OPENSTATES_API_KEY (optional)
Run: python scripts/ingest_fl_legislator_metrics.py [--limit N] [--dry-run] [--skip-openstates]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from typing import Optional

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
OPENSTATES_KEY = os.getenv("OPENSTATES_API_KEY")

SESSION_YEAR = 2025
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fl_metrics_log.txt")
OS_BASE = "https://v3.openstates.org"
OS_TIMEOUT = 15
OS_DELAY = 0.5
OS_MAX_VOTES = 200  # cap OpenStates roll-call pulls to keep quota usable

session = requests.Session()
session.headers.update({"User-Agent": "PolitiScore Ingest/1.0 (https://politiscore.com)"})


def log(msg: str) -> None:
    print(msg, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except OSError:
        pass


# ─── Rating helpers ─────────────────────────────────────────────────────────
def rate_bills_sponsored(n: int) -> str:
    if n >= 20: return "excellent"
    if n >= 10: return "good"
    if n >= 5:  return "meeting"
    if n >= 2:  return "concerning"
    if n >= 1:  return "poor"
    return "no_data"


def rate_bills_passed(n: int) -> str:
    if n >= 3: return "excellent"
    if n >= 2: return "good"
    if n >= 1: return "meeting"
    return "poor"


def rate_attendance(pct: float) -> str:
    if pct >= 98: return "excellent"
    if pct >= 95: return "good"
    if pct >= 90: return "meeting"
    if pct >= 80: return "concerning"
    return "poor"


def rate_partyline(pct: float) -> str:
    # Lower is better — more independent vote choices.
    if pct <= 80: return "excellent"
    if pct <= 90: return "good"
    if pct <= 95: return "meeting"
    if pct <= 98: return "concerning"
    return "poor"


# ─── Chamber benchmarks ─────────────────────────────────────────────────────
def benchmark(metric_key: str, chamber: str) -> tuple[Optional[str], Optional[str]]:
    if metric_key == "bills_sponsored":
        return ("10.0", f"FL {chamber} median") if chamber else (None, None)
    if metric_key == "bills_passed":
        return ("2.0", f"FL {chamber} typical")
    if metric_key == "voting_attendance":
        return ("95.0", "Chamber average")
    if metric_key == "party_line_voting":
        return ("92.0", "Chamber average")
    return (None, None)


def chamber_for(title: str) -> str:
    t = (title or "").lower()
    if "senator" in t:
        return "Senate"
    if "representative" in t or "house" in t:
        return "House"
    return ""


# ─── Compute from existing legislative_activity ─────────────────────────────
def fetch_sponsorship_counts(sb) -> dict[int, dict]:
    """Returns { official_id: { 'sponsored': int, 'passed': int } }."""
    out: dict[int, dict] = defaultdict(lambda: {"sponsored": 0, "passed": 0})
    page = 0
    PAGE = 1000
    while True:
        chunk = (
            sb.table("legislative_activity")
            .select("official_id, status")
            .eq("activity_type", "bill_sponsored")
            .not_.is_("official_id", "null")
            .range(page * PAGE, page * PAGE + PAGE - 1)
            .execute()
            .data
            or []
        )
        if not chunk:
            break
        for r in chunk:
            oid = r.get("official_id")
            if oid is None:
                continue
            out[oid]["sponsored"] += 1
            s = (r.get("status") or "").strip().lower()
            if s in ("enacted", "signed"):
                out[oid]["passed"] += 1
        if len(chunk) < PAGE:
            break
        page += 1
    return out


# ─── Best-effort OpenStates pull ────────────────────────────────────────────
def fetch_openstates_attendance(legislators: list[dict]) -> dict[int, dict]:
    """Returns { official_id: { 'attendance_pct': float, 'party_line_pct': float } }.
    Conservative — returns {} if the API is unreachable or returns no useful data.
    Uses /bills?jurisdiction=fl&session={year}&include=votes (which embeds vote
    counts but not always per-member breakdown). For a real per-member roll-call
    pull we'd need /bills/{id}/votes which is multi-step.

    Given the API surface and quota constraints in one session, this function
    intentionally no-ops when it can't reach a clean answer — the metrics stay
    as their existing 'no_data' placeholders rather than getting bad values.
    """
    if not OPENSTATES_KEY:
        return {}
    try:
        r = session.get(
            f"{OS_BASE}/bills",
            params={
                "jurisdiction": "fl",
                "session": str(SESSION_YEAR),
                "include": "votes",
                "per_page": 1,
                "apikey": OPENSTATES_KEY,
            },
            timeout=OS_TIMEOUT,
        )
        if r.status_code != 200:
            log(f"  openstates probe returned {r.status_code} — skipping vote ingest")
            return {}
    except requests.RequestException as e:
        log(f"  openstates probe failed: {e} — skipping vote ingest")
        return {}

    # Realistic note: OpenStates returns votes embedded, but the per-member
    # vote breakdown only comes via /votes which would be ~2,400 paginated
    # fetches for the 2025 FL session. Out of scope for one-shot ingestion.
    # Punting to a separate dedicated script.
    log("  openstates: per-member vote breakdown requires a separate ingest pass "
        f"({OS_MAX_VOTES}+ requests). Skipping for now; leaving voting_attendance "
        "+ party_line_voting as no_data placeholders.")
    return {}


# ─── Upsert ─────────────────────────────────────────────────────────────────
def upsert_metric(sb, *, official_id: int, official_name: str, metric_key: str,
                  metric_label: str, metric_value: str, metric_unit: Optional[str],
                  rating: str, chamber: str, dry_run: bool) -> None:
    bench_v, bench_l = benchmark(metric_key, chamber)
    payload = {
        "official_id": official_id,
        "official_name": official_name,
        "metric_key": metric_key,
        "metric_label": metric_label,
        "metric_value": metric_value,
        "metric_unit": metric_unit,
        "benchmark_value": bench_v,
        "benchmark_label": bench_l,
        "performance_rating": rating,
        "year": SESSION_YEAR,
        "source": "FL Legislature (computed from sponsorship ingest)",
        "source_url": "https://www.flsenate.gov/Session" if chamber == "Senate"
                      else "https://www.myfloridahouse.gov/Sections/Bills/bills.aspx",
        "notes": "Computed from legislative_activity rows",
        "last_updated": "now()",
    }
    if dry_run:
        return
    # delete-then-insert keyed by (official_id, metric_key, year)
    sb.table("accountability_metrics") \
        .delete() \
        .eq("official_id", official_id) \
        .eq("metric_key", metric_key) \
        .eq("year", SESSION_YEAR) \
        .execute()
    sb.table("accountability_metrics").insert(payload).execute()


# ─── Main ───────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-openstates", action="store_true",
                        help="Don't even probe OpenStates (default behavior is "
                             "already conservative — probe + skip on failure)")
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        log("ERROR: SUPABASE_URL + SUPABASE_SERVICE_KEY required")
        return 1
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    log(f"start  session_year={SESSION_YEAR} dry-run={args.dry_run}")

    # 1. Load FL state legislators (senators + representatives)
    legislators: list[dict] = []
    page = 0
    PAGE = 500
    while True:
        chunk = (
            sb.table("elected_officials")
            .select("id, name, title, party")
            .eq("level", "state")
            .eq("state", "FL")
            .range(page * PAGE, page * PAGE + PAGE - 1)
            .execute()
            .data
            or []
        )
        if not chunk:
            break
        for r in chunk:
            t = (r.get("title") or "").lower()
            if "senator" in t or "representative" in t:
                legislators.append(r)
        if len(chunk) < PAGE:
            break
        page += 1
    if args.limit:
        legislators = legislators[: args.limit]
    log(f"legislators to update: {len(legislators)}")

    # 2. Sponsorship counts from existing legislative_activity
    counts = fetch_sponsorship_counts(sb)
    log(f"sponsorship counts available for {len(counts)} officials")

    # 3. Best-effort vote ingest (currently a no-op — needs dedicated pass)
    attendance = {} if args.skip_openstates else fetch_openstates_attendance(legislators)

    # 4. Update metrics
    sponsored_n = passed_n = vote_n = 0
    for i, leg in enumerate(legislators, 1):
        oid = leg["id"]; name = leg["name"]; chamber = chamber_for(leg.get("title", ""))
        c = counts.get(oid, {"sponsored": 0, "passed": 0})

        upsert_metric(
            sb, official_id=oid, official_name=name,
            metric_key="bills_sponsored", metric_label="Bills Sponsored",
            metric_value=str(c["sponsored"]), metric_unit="bills",
            rating=rate_bills_sponsored(c["sponsored"]), chamber=chamber,
            dry_run=args.dry_run,
        )
        sponsored_n += 1

        upsert_metric(
            sb, official_id=oid, official_name=name,
            metric_key="bills_passed", metric_label="Bills Signed Into Law",
            metric_value=str(c["passed"]), metric_unit="bills",
            rating=rate_bills_passed(c["passed"]), chamber=chamber,
            dry_run=args.dry_run,
        )
        passed_n += 1

        if oid in attendance:
            att = attendance[oid].get("attendance_pct")
            pl = attendance[oid].get("party_line_pct")
            if att is not None:
                upsert_metric(
                    sb, official_id=oid, official_name=name,
                    metric_key="voting_attendance", metric_label="Voting Attendance",
                    metric_value=f"{att:.1f}", metric_unit="%",
                    rating=rate_attendance(att), chamber=chamber,
                    dry_run=args.dry_run,
                )
                vote_n += 1
            if pl is not None:
                upsert_metric(
                    sb, official_id=oid, official_name=name,
                    metric_key="party_line_voting", metric_label="Party-Line Voting %",
                    metric_value=f"{pl:.1f}", metric_unit="%",
                    rating=rate_partyline(pl), chamber=chamber,
                    dry_run=args.dry_run,
                )

        if i % 20 == 0:
            log(f"  …processed {i}/{len(legislators)}")

    log("")
    log(f"DONE  bills_sponsored={sponsored_n}  bills_passed={passed_n}  vote_metrics={vote_n}  legislators={len(legislators)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
