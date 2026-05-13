"""OpenStates roll-call voting history ingest for FL state legislators.

Pulls every FL bill from the configured session(s) with embedded vote
data, aggregates per-member, and computes:

  voting_attendance %  = (votes_cast / total_chamber_votes) * 100
  party_line_voting %  = (votes_with_party / votes_cast) * 100

"Party-line" is decided per-vote: the position of a member's party
caucus majority on that vote = the party line. If a member voted with
that majority position, the vote counts toward their party_line tally.

Then upserts performance_rating into accountability_metrics for every
matched FL state legislator. Idempotent — keyed on
(official_id, metric_key, year).

Env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY, OPENSTATES_API_KEY required.

Run:
  python scripts/ingest_fl_voting_history.py
  python scripts/ingest_fl_voting_history.py --session 2024 --dry-run
  python scripts/ingest_fl_voting_history.py --max-pages 10
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
OS_KEY = os.getenv("OPENSTATES_API_KEY")

OS_BASE = "https://v3.openstates.org"
OS_DELAY = 0.6  # free tier is loose but be polite
PER_PAGE = 20

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fl_voting_log.txt")

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
def rate_attendance(pct: float) -> str:
    if pct >= 98: return "excellent"
    if pct >= 95: return "good"
    if pct >= 90: return "meeting"
    if pct >= 80: return "concerning"
    return "poor"


def rate_partyline(pct: float) -> str:
    # Lower = more independent → better. The "poor" cutoff is >99% because
    # FL's chamber is structurally polarized — almost everyone clears 95-98%,
    # so the cutoff at 99 separates true rubber-stampers from the rest.
    if pct <= 80: return "excellent"
    if pct <= 90: return "good"
    if pct <= 95: return "meeting"
    if pct <= 99: return "concerning"
    return "poor"


# ─── OpenStates pagination ──────────────────────────────────────────────────
def fetch_session_bills(session_year: str, max_pages: Optional[int] = None) -> list[dict]:
    """Returns the full set of FL bills for the given session with embedded
    vote events. One paginated /bills?include=votes call per page."""
    bills: list[dict] = []
    page = 1
    while True:
        try:
            r = session.get(
                f"{OS_BASE}/bills",
                params={
                    "jurisdiction": "fl",
                    "session": session_year,
                    "include": "votes",
                    "per_page": PER_PAGE,
                    "page": page,
                    "apikey": OS_KEY,
                },
                timeout=20,
            )
        except requests.RequestException as e:
            log(f"  page {page} fetch error: {e}")
            break
        if r.status_code == 429:
            log(f"  page {page} rate-limited (429), sleeping 10s and retrying")
            time.sleep(10)
            continue
        if r.status_code != 200:
            log(f"  page {page} returned {r.status_code} — stopping")
            break
        try:
            data = r.json()
        except ValueError:
            log(f"  page {page} returned non-JSON; stopping")
            break

        chunk = data.get("results") or []
        bills.extend(chunk)

        pg = data.get("pagination") or {}
        max_page = pg.get("max_page", 0)
        log(f"  page {page}/{max_page}: +{len(chunk)} bills (total {len(bills)})")
        if max_pages and page >= max_pages:
            log("  hit --max-pages cap")
            break
        if page >= max_page or not chunk:
            break
        page += 1
        time.sleep(OS_DELAY)
    return bills


# ─── Per-member aggregation ─────────────────────────────────────────────────
def aggregate_voting(bills: list[dict]) -> dict[str, dict]:
    """Returns { normalized_name: {
        'party': str | None,
        'chamber_role': str | None,
        'votes_cast': int,
        'total_chamber_votes': int,
        'party_line_votes': int,
        'party_line_eligible': int,   # votes where party caucus had a clear majority
    } }.

    Party-line eligibility: a vote counts toward the party-line tally only when
    the member's party caucus had a clear majority position (yes or no). Ties /
    unanimous votes / "not voting"-heavy bills are excluded so they don't
    artificially inflate or deflate the rating.
    """
    agg: dict[str, dict] = defaultdict(lambda: {
        "party": None, "chamber_role": None,
        "votes_cast": 0, "total_chamber_votes": 0,
        "party_line_votes": 0, "party_line_eligible": 0,
    })
    total_votes_processed = 0
    skipped_no_members = 0

    for bill in bills:
        for v in bill.get("votes") or []:
            members = v.get("votes") or []
            if not members:
                skipped_no_members += 1
                continue
            total_votes_processed += 1

            # 1. Per-party tally for this vote (yes / no / other)
            party_yes: defaultdict = defaultdict(int)
            party_no: defaultdict = defaultdict(int)
            for m in members:
                p = ((m.get("voter") or {}).get("party") or "Unknown").strip() or "Unknown"
                opt = (m.get("option") or "").lower()
                if opt == "yes":
                    party_yes[p] += 1
                elif opt == "no":
                    party_no[p] += 1

            # Determine "party line" position per party for this vote.
            #   yes if majority of voting party members said yes
            #   no  if majority said no
            #   None for ties or empty
            party_line: dict[str, Optional[str]] = {}
            for p in set(party_yes) | set(party_no):
                y = party_yes.get(p, 0); n = party_no.get(p, 0)
                if y > n: party_line[p] = "yes"
                elif n > y: party_line[p] = "no"
                else: party_line[p] = None

            # 2. Walk members again, update aggregates
            for m in members:
                voter = m.get("voter") or {}
                # Prefer the OpenStates-canonical voter.name; fall back to voter_name
                name = (voter.get("name") or m.get("voter_name") or "").strip()
                if not name:
                    continue
                key = name.lower()
                party = (voter.get("party") or "Unknown").strip() or "Unknown"
                role = ((voter.get("current_role") or {}).get("title") or "").strip() or None

                bucket = agg[key]
                bucket["party"] = party
                bucket["chamber_role"] = role
                bucket["total_chamber_votes"] += 1

                opt = (m.get("option") or "").lower()
                if opt in ("yes", "no", "other"):  # treat "other" as cast (rare)
                    bucket["votes_cast"] += 1

                # Party-line eligibility & tally
                pl = party_line.get(party)
                if pl is not None and opt in ("yes", "no"):
                    bucket["party_line_eligible"] += 1
                    if opt == pl:
                        bucket["party_line_votes"] += 1

    log(f"  processed {total_votes_processed} votes; skipped {skipped_no_members} (no member data)")
    return agg


# ─── Match aggregates to elected_officials ─────────────────────────────────
def load_state_legislators(sb) -> dict[str, dict]:
    """{ lower-name: {id, name, title, party} } for the 156 FL state
    senators + representatives we track."""
    out: dict[str, dict] = {}
    page = 0; PAGE = 500
    while True:
        chunk = (
            sb.table("elected_officials")
            .select("id, name, title, party")
            .eq("level", "state")
            .eq("state", "FL")
            .range(page * PAGE, page * PAGE + PAGE - 1)
            .execute()
            .data or []
        )
        if not chunk:
            break
        for r in chunk:
            t = (r.get("title") or "").lower()
            if "senator" in t or "representative" in t:
                key = (r.get("name") or "").strip().lower()
                if key:
                    out[key] = r
        if len(chunk) < PAGE:
            break
        page += 1
    return out


def upsert_metric(sb, *, official_id: int, official_name: str, metric_key: str,
                  metric_label: str, metric_value: str, rating: str,
                  benchmark_value: str, benchmark_label: str,
                  session_year: int, dry_run: bool, source_url: str) -> None:
    payload = {
        "official_id": official_id,
        "official_name": official_name,
        "metric_key": metric_key,
        "metric_label": metric_label,
        "metric_value": metric_value,
        "metric_unit": "%",
        "benchmark_value": benchmark_value,
        "benchmark_label": benchmark_label,
        "performance_rating": rating,
        "year": session_year,
        "source": "OpenStates (FL roll-call votes)",
        "source_url": source_url,
        "notes": "Computed from FL Legislature roll-call vote data via OpenStates API",
        "last_updated": "now()",
    }
    if dry_run:
        return
    sb.table("accountability_metrics") \
        .delete() \
        .eq("official_id", official_id) \
        .eq("metric_key", metric_key) \
        .eq("year", session_year) \
        .execute()
    sb.table("accountability_metrics").insert(payload).execute()


# ─── Main ───────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", default="2025", help="FL session year (default 2025)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-pages", type=int, default=None, help="Cap pagination (debug)")
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY and OS_KEY):
        log("ERROR: SUPABASE_URL + SUPABASE_SERVICE_KEY + OPENSTATES_API_KEY required")
        return 1

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    session_year = int(args.session)

    log(f"start session={args.session} dry-run={args.dry_run} max_pages={args.max_pages}")

    # 1. Paginate all bills with vote data
    bills = fetch_session_bills(args.session, max_pages=args.max_pages)
    log(f"bills loaded: {len(bills)}")

    # 2. Aggregate per-member
    agg = aggregate_voting(bills)
    log(f"unique voters seen: {len(agg)}")

    # 3. Match to elected_officials
    legislators = load_state_legislators(sb)
    log(f"FL state legislators in DB: {len(legislators)}")

    matched = unmatched = 0
    attendance_n = partyline_n = 0
    source_url = f"https://openstates.org/fl/bills/?session={args.session}"

    for key, leg in legislators.items():
        a = agg.get(key)
        if not a:
            # Try last-name fuzzy match as fallback
            last = (leg.get("name") or "").strip().split()[-1].lower()
            for ak in agg:
                if ak.endswith(last):
                    a = agg[ak]
                    break
        if not a or a["total_chamber_votes"] == 0:
            unmatched += 1
            continue
        matched += 1

        attendance_pct = (a["votes_cast"] / a["total_chamber_votes"]) * 100.0
        upsert_metric(
            sb, official_id=leg["id"], official_name=leg["name"],
            metric_key="voting_attendance", metric_label="Voting Attendance",
            metric_value=f"{attendance_pct:.1f}",
            rating=rate_attendance(attendance_pct),
            benchmark_value="95.0", benchmark_label="Chamber average",
            session_year=session_year, dry_run=args.dry_run,
            source_url=source_url,
        )
        attendance_n += 1

        if a["party_line_eligible"] > 0:
            pl_pct = (a["party_line_votes"] / a["party_line_eligible"]) * 100.0
            upsert_metric(
                sb, official_id=leg["id"], official_name=leg["name"],
                metric_key="party_line_voting", metric_label="Party-Line Voting %",
                metric_value=f"{pl_pct:.1f}",
                rating=rate_partyline(pl_pct),
                benchmark_value="92.0", benchmark_label="Chamber average",
                session_year=session_year, dry_run=args.dry_run,
                source_url=source_url,
            )
            partyline_n += 1

    log("")
    log(f"DONE  matched={matched}  unmatched={unmatched}  "
        f"attendance_written={attendance_n}  party_line_written={partyline_n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
