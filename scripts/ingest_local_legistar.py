"""Ingest local-government legislative activity from Granicus Legistar.

Pulls last 365 days of events + their eventitems for each working FL Legistar
tenant (currently broward, pinellas; miamidade is frozen at 2018), then
attributes per-official action by matching the EventItem Mover / Seconder
names — and any roll-call /votes records — to elected_officials by fuzzy
last-name + first-name token match.

Inserts into legislative_activity with official_id populated. Idempotent
via the existing unique index on
(official_name, activity_type, bill_number, title, vote_position).

Activity-type mapping (per product spec):
  - vote position recorded in /eventitems/{id}/votes  -> activity_type='vote'
  - mover/seconder recorded in eventitem fields       -> activity_type='bill_sponsored'

Usage:
  python scripts/ingest_local_legistar.py
  python scripts/ingest_local_legistar.py --counties broward
  python scripts/ingest_local_legistar.py --days 90
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

LEGISTAR_BASE = "https://webapi.legistar.com/v1"
REQ_DELAY = 0.4  # polite default
HTTP_TIMEOUT = 30

# Live FL Legistar tenants. miamidade returns 200 but data is frozen at
# mid-2018, so it's intentionally excluded.
DEFAULT_COUNTIES = ("broward", "pinellas")

LOG = logging.getLogger("legistar")
if not LOG.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(h)
    fh = logging.FileHandler("ingest_local_legistar.log", mode="a")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(fh)
    LOG.setLevel(logging.INFO)

UA = {"User-Agent": "PolitiCard-ingest/0.1", "Accept": "application/json"}


# --- HTTP helper ----------------------------------------------------------

def http_get(url: str, params: dict | None = None, retries: int = 3) -> object | None:
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 502, 503, 504) and attempt < retries:
                time.sleep(2 ** attempt)
                continue
            LOG.warning("HTTP %s %s params=%s body=%s", r.status_code, url, params, r.text[:120])
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            LOG.warning("HTTP error %s for %s: %s", attempt, url, e)
            return None
    return None


# --- Name normalization & matching ----------------------------------------

_SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv|v|esq|md|phd|cpa)\.?$", re.IGNORECASE)
_TITLE_RE = re.compile(r"^(commissioner|comm\.|councilm[ae]n|councilwoman|mayor|chair|vice\s*chair|vice\s*mayor|hon\.|the\s+honorable|dr|mr|ms|mrs)\s+", re.IGNORECASE)


def _norm_tokens(s: str) -> list[str]:
    """Lowercase, ASCII-fold, drop titles/suffixes/punct, return word tokens."""
    if not s:
        return []
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("'", "").replace("-", " ")
    # Drop a leading title
    s = _TITLE_RE.sub("", s.strip())
    # Drop a trailing suffix
    s = _SUFFIX_RE.sub("", s).strip()
    s = re.sub(r"[^A-Za-z\s]", " ", s).lower()
    return [t for t in s.split() if len(t) > 1]


def build_official_index(supabase) -> dict[tuple[str, str], list[dict]]:
    """Return {(first_initial, last): [{id, name, title}]} for every FL local official."""
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, level, state")
            .eq("state", "FL")
            .eq("level", "local")
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000

    idx: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for o in rows:
        toks = _norm_tokens(o.get("name") or "")
        if len(toks) < 2:
            continue
        key = (toks[0][0], toks[-1])  # (first_initial, last)
        idx[key].append(o)
    LOG.info("Indexed %d FL local officials (%d unique first-initial+last keys)", len(rows), len(idx))
    return idx


def match_official(name: str, idx: dict[tuple[str, str], list[dict]]) -> dict | None:
    """Fuzzy match an official by first-initial + last name. Returns None on
    miss or ambiguity (multiple officials with same first-initial+last)."""
    toks = _norm_tokens(name)
    if len(toks) < 2:
        return None
    key = (toks[0][0], toks[-1])
    cands = idx.get(key, [])
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0]
    # Disambiguate: require full first-name match
    full_first = toks[0]
    refined = [
        c for c in cands
        if (c_toks := _norm_tokens(c.get("name") or "")) and c_toks[0] == full_first
    ]
    if len(refined) == 1:
        return refined[0]
    return None  # ambiguous


# --- Vote position normalization ------------------------------------------

def _normalize_vote(value: str) -> str | None:
    if not value:
        return None
    v = value.strip().lower()
    if v in ("yes", "yea", "aye", "y", "approve"):
        return "Yea"
    if v in ("no", "nay", "n", "deny", "reject"):
        return "Nay"
    if v in ("abstain", "abstention", "recuse", "recused"):
        return "Abstain"
    if v in ("absent", "excused", "out"):
        return "Absent"
    if v in ("present",):
        return "Present"
    return value.strip().title()[:50] or None


# --- Persistence ----------------------------------------------------------

def _row(*, official, county_slug, activity_type, title, bill_number,
         description=None, vote_position=None, status=None, date=None,
         source_url=None) -> dict:
    return {
        "official_id": official["id"],
        "official_name": official["name"],
        "official_level": "local",
        "chamber": "local",
        "activity_type": activity_type,
        "bill_number": bill_number or None,
        "title": (title or "").strip()[:1000] or "(untitled)",
        "description": (description or "").strip()[:2000] or None,
        "status": status,
        "vote_position": vote_position,
        "date": date,
        "source": f"Legistar ({county_slug})",
        "source_url": source_url,
        "state": "FL",
    }


def insert_rows(supabase, rows: list[dict]) -> int:
    if not rows:
        return 0
    try:
        supabase.table("legislative_activity").upsert(
            rows,
            ignore_duplicates=True,
            on_conflict="official_name,activity_type,bill_number,title,vote_position",
        ).execute()
        return len(rows)
    except Exception as e:
        LOG.warning("Bulk upsert failed (%s); retrying row-by-row", e)
        ok = 0
        for r in rows:
            try:
                supabase.table("legislative_activity").upsert(
                    r,
                    ignore_duplicates=True,
                    on_conflict="official_name,activity_type,bill_number,title,vote_position",
                ).execute()
                ok += 1
            except Exception as e2:
                LOG.warning("Row insert failed: %s", e2)
        return ok


def _chunked(items: list[dict], n: int = 200):
    for i in range(0, len(items), n):
        yield items[i:i + n]


# --- Legistar pull -------------------------------------------------------

def fetch_events(slug: str, since: str, until: str) -> list[dict]:
    """All events with EventDate in [since, until]. Paginated."""
    out: list[dict] = []
    skip = 0
    page = 1000
    flt = f"EventDate ge datetime'{since}' and EventDate le datetime'{until}'"
    while True:
        d = http_get(
            f"{LEGISTAR_BASE}/{slug}/events",
            params={"$filter": flt, "$top": str(page), "$skip": str(skip),
                    "$orderby": "EventDate desc"},
        )
        time.sleep(REQ_DELAY)
        if not isinstance(d, list) or not d:
            break
        out.extend(d)
        if len(d) < page:
            break
        skip += page
    return out


def fetch_eventitems(slug: str, event_id: int) -> list[dict]:
    d = http_get(f"{LEGISTAR_BASE}/{slug}/events/{event_id}/eventitems")
    time.sleep(REQ_DELAY)
    return d if isinstance(d, list) else []


def fetch_eventitem_votes(slug: str, eventitem_id: int) -> list[dict]:
    d = http_get(f"{LEGISTAR_BASE}/{slug}/eventitems/{eventitem_id}/votes")
    time.sleep(REQ_DELAY)
    return d if isinstance(d, list) else []


# --- Per-county processor -------------------------------------------------

def process_county(supabase, slug: str, idx: dict, days: int) -> dict:
    today = datetime.now(timezone.utc).date()
    since = (today - timedelta(days=days)).isoformat()
    # Allow scheduled future events up to 30 days out, drop far-future placeholders
    until = (today + timedelta(days=30)).isoformat()
    LOG.info("[%s] Fetching events %s .. %s", slug, since, until)

    events = fetch_events(slug, since, until)
    LOG.info("[%s] events: %d", slug, len(events))

    matched_officials: set[int] = set()
    inserted = 0
    item_rows: list[dict] = []
    rollcall_calls = 0

    for ev_i, ev in enumerate(events, 1):
        eid = ev.get("EventId")
        if not eid:
            continue
        items = fetch_eventitems(slug, eid)
        if not items:
            continue
        ev_date = (ev.get("EventDate") or "")[:10] or None
        ev_body = ev.get("EventBodyName") or ""
        for ei in items:
            bill_number = (ei.get("EventItemMatterFile") or "").strip() or None
            title = (ei.get("EventItemMatterName") or ei.get("EventItemAgendaNote")
                     or ei.get("EventItemActionText") or "(item)").strip()
            action = (ei.get("EventItemActionName") or "").strip() or None
            passed = ei.get("EventItemPassedFlagName")
            status = "passed_chamber" if (passed and "pass" in passed.lower()) else (
                "failed" if (passed and "fail" in passed.lower()) else None)
            source_url = (
                f"https://{slug}.legistar.com/LegislationDetail.aspx?"
                f"ID={ei.get('EventItemMatterId')}" if ei.get("EventItemMatterId") else None
            )

            # Mover / Seconder rows: bill_sponsored
            for role_field, role_label in (
                ("EventItemMover", "Motion mover"),
                ("EventItemSeconder", "Motion seconder"),
            ):
                voter_name = (ei.get(role_field) or "").strip()
                if not voter_name:
                    continue
                off = match_official(voter_name, idx)
                if not off:
                    continue
                matched_officials.add(off["id"])
                item_rows.append(_row(
                    official=off,
                    county_slug=slug,
                    activity_type="bill_sponsored",
                    title=title,
                    bill_number=bill_number,
                    description=f"{role_label} on {ev_body} ({action or 'agenda item'})",
                    status=status,
                    date=ev_date,
                    source_url=source_url,
                ))

            # Roll-call votes — only fetch if flagged, else /votes is empty anyway.
            if ei.get("EventItemRollCallFlag"):
                rollcall_calls += 1
                votes = fetch_eventitem_votes(slug, ei.get("EventItemId"))
                for v in votes:
                    # Legistar typically returns VotePersonName + VoteValueName (Yes/No/Abstain/...)
                    voter_name = v.get("VotePersonName") or v.get("VoteVoterName") or ""
                    pos = _normalize_vote(v.get("VoteValueName") or v.get("VoteResult") or "")
                    if not voter_name or not pos:
                        continue
                    off = match_official(voter_name, idx)
                    if not off:
                        continue
                    matched_officials.add(off["id"])
                    item_rows.append(_row(
                        official=off,
                        county_slug=slug,
                        activity_type="vote",
                        title=title,
                        bill_number=bill_number,
                        description=f"{ev_body} — {action or 'roll-call vote'}",
                        vote_position=pos,
                        status=status,
                        date=ev_date,
                        source_url=source_url,
                    ))

        # Periodically flush so a crash doesn't lose everything
        if len(item_rows) >= 500:
            for chunk in _chunked(item_rows, 200):
                inserted += insert_rows(supabase, chunk)
            item_rows = []

        if ev_i % 25 == 0:
            LOG.info("[%s] processed %d/%d events; rows-staged-so-far=%d, inserted=%d",
                     slug, ev_i, len(events), len(item_rows), inserted)

    # Final flush
    for chunk in _chunked(item_rows, 200):
        inserted += insert_rows(supabase, chunk)

    LOG.info("[%s] DONE — events=%d rollcall_fetches=%d officials_matched=%d rows_inserted=%d",
             slug, len(events), rollcall_calls, len(matched_officials), inserted)
    return {
        "events": len(events),
        "rollcall_fetches": rollcall_calls,
        "officials_matched": len(matched_officials),
        "rows_inserted": inserted,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--counties", default=",".join(DEFAULT_COUNTIES),
                        help="comma-separated Legistar slugs")
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
        return 1
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    idx = build_official_index(supabase)

    counties = [c.strip() for c in args.counties.split(",") if c.strip()]
    summary: dict[str, dict] = {}
    for slug in counties:
        try:
            summary[slug] = process_county(supabase, slug, idx, args.days)
        except Exception as e:
            LOG.exception("Failed county %s: %s", slug, e)
            summary[slug] = {"error": str(e)}

    LOG.info("=== FINAL SUMMARY ===")
    for slug, s in summary.items():
        LOG.info("  %s: %s", slug, s)
    return 0


if __name__ == "__main__":
    sys.exit(main())
