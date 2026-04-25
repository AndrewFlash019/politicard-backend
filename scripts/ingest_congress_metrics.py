"""Session 1 ingestion: Congress.gov + GovTrack + OpenStates → accountability_metrics.

Fills real performance data for FL federal legislators (2 senators + 26 reps)
and FL state legislators (~39 senators + ~117 reps).

Federal metrics (per official, year=current_session):
  - bills_sponsored        Congress.gov  /v3/member/{bioguide}/sponsored-legislation
  - bills_passed           Congress.gov  (filter sponsored by latestAction "Public Law")
  - voting_attendance      GovTrack      /v2/vote_voter (count vs option__key=0)
  - committee_assignments  unitedstates  committee-membership-current.json
  - party_line_voting      computed from a sample of recent votes via GovTrack

State metrics (per official, year=current_session):
  - bills_sponsored        OpenStates    /bills?sponsor={pid}&session=2025
  - bills_passed           OpenStates    (filter latest_action_description for signed/Ch.)

Requires env: CONGRESS_API_KEY, OPENSTATES_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY.
Run: python scripts/ingest_congress_metrics.py
     python scripts/ingest_congress_metrics.py --only federal
     python scripts/ingest_congress_metrics.py --only state --limit 5
"""

from __future__ import annotations

import argparse
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

LOG = logging.getLogger("ingest_metrics")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest_congress_metrics.log", mode="a"),
    ],
)

CONGRESS_KEY = os.getenv("CONGRESS_API_KEY")
OPENSTATES_KEY = os.getenv("OPENSTATES_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

CURRENT_CONGRESS = 119
CURRENT_FED_YEAR = 2025  # 119th Congress data (Jan 2025 onward)
CURRENT_STATE_YEAR = 2025
# Use only the most recent regular session to stay within OpenStates 500/day quota.
STATE_SESSIONS = ["2025"]
GOVTRACK_119_START = "2025-01-03"

CONGRESS_BASE = "https://api.congress.gov/v3"
GOVTRACK_BASE = "https://www.govtrack.us/api/v2"
OPENSTATES_BASE = "https://v3.openstates.org"
USDS_COMMITTEES = "https://unitedstates.github.io/congress-legislators/committees-current.json"
USDS_MEMBERSHIP = "https://unitedstates.github.io/congress-legislators/committee-membership-current.json"

USER_AGENT = "PolitiScore Civic Data Bot +https://politiscore.com"

# Pacing
CONGRESS_DELAY = 1.1
GOVTRACK_DELAY = 1.1
OPENSTATES_DELAY = 1.5

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# --- Rating helpers --------------------------------------------------------


def rate_bills_sponsored(count: int, chamber: str) -> tuple[str, float, str]:
    """Return (rating, benchmark, benchmark_label) for a sponsored-bill count."""
    if chamber == "senate":
        median = 30.0
    else:
        median = 50.0
    if count > 75:
        rating = "excellent"
    elif count > 50:
        rating = "good"
    elif count > 25:
        rating = "meeting"
    elif count > 10:
        rating = "concerning"
    else:
        rating = "poor"
    return rating, median, f"{chamber.title()} median"


def rate_bills_passed(count: int) -> str:
    if count >= 5:
        return "excellent"
    if count >= 2:
        return "good"
    if count >= 1:
        return "meeting"
    return "poor"


def rate_attendance(pct: float) -> str:
    if pct > 97:
        return "excellent"
    if pct > 95:
        return "good"
    if pct > 92:
        return "meeting"
    if pct > 85:
        return "concerning"
    return "poor"


def rate_party_line(pct: float) -> str:
    # 80-92 balanced; <80 too independent; >95 too partisan
    if 80 <= pct <= 92:
        return "good"
    if 70 <= pct < 80 or 92 < pct <= 95:
        return "meeting"
    return "concerning"


def rate_state_bills_sponsored(count: int, chamber: str) -> tuple[str, float, str]:
    median = 15.0 if chamber == "upper" else 10.0
    if count >= 30:
        rating = "excellent"
    elif count >= 15:
        rating = "good"
    elif count >= 7:
        rating = "meeting"
    elif count >= 3:
        rating = "concerning"
    else:
        rating = "poor"
    return rating, median, f"FL {('Senate' if chamber == 'upper' else 'House')} median"


# --- HTTP helpers ----------------------------------------------------------


def http_get(url: str, params: dict | None = None, timeout: int = 25, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = 2 ** attempt * 5
                LOG.warning("429 rate-limited, sleeping %ds: %s", wait, url[:80])
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = 2 ** attempt * 2
                LOG.warning("HTTP %d, retry in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            LOG.warning("HTTP error attempt %d: %s — %s", attempt + 1, url[:80], e)
            time.sleep(2)
    return None


# --- Federal: Congress.gov + GovTrack -------------------------------------


_fl_members_cache: list[dict] | None = None


def fetch_fl_congress_members() -> list[dict]:
    """Return list of FL members of the current congress."""
    global _fl_members_cache
    if _fl_members_cache is not None:
        return _fl_members_cache
    members: list[dict] = []
    offset = 0
    while True:
        d = http_get(
            f"{CONGRESS_BASE}/member/congress/{CURRENT_CONGRESS}/FL",
            params={"api_key": CONGRESS_KEY, "format": "json", "limit": 250, "offset": offset},
        )
        time.sleep(CONGRESS_DELAY)
        if not d:
            break
        items = d.get("members", [])
        if not items:
            break
        members.extend(items)
        if len(items) < 250:
            break
        offset += 250
    _fl_members_cache = members
    LOG.info("Cached %d FL congress members", len(members))
    return members


def _name_tokens(s: str) -> list[str]:
    """Lowercase, ASCII-fold, strip punctuation, split into word tokens."""
    import unicodedata as _ud
    folded = _ud.normalize("NFKD", s)
    folded = "".join(c for c in folded if not _ud.combining(c))
    folded = re.sub(r"[^A-Za-z\s]", " ", folded).lower()
    return [t for t in folded.split() if len(t) > 1]


def find_bioguide(name: str, chamber_hint: str, district: str | None = None) -> str | None:
    """Match an elected_official name to a Congress.gov bioguideId.

    Strategy: tokenize names, require last-name match plus any first-name
    overlap (allowing prefix/contains for nicknames like "Greg" vs "Gregory").
    Optional district hint disambiguates ties for House members.
    """
    members = fetch_fl_congress_members()
    target_chamber = "house" if "house" in chamber_hint.lower() else "senate"
    target_tokens = _name_tokens(name)
    if not target_tokens:
        return None
    target_last = target_tokens[-1]
    target_firsts = target_tokens[:-1]

    candidates: list[tuple[int, str]] = []
    for m in members:
        raw = m.get("name", "")
        parts = [p.strip() for p in raw.split(",", 1)]
        if len(parts) >= 2:
            full = f"{parts[1]} {parts[0]}"
        else:
            full = raw
        cand_tokens = _name_tokens(full)
        if not cand_tokens:
            continue
        cand_last = cand_tokens[-1]
        cand_firsts = cand_tokens[:-1]

        # Chamber filter
        terms = m.get("terms", {})
        items = terms.get("item", []) if isinstance(terms, dict) else (terms or [])
        latest_chamber = (items[-1].get("chamber") or "").lower() if items else ""
        if target_chamber not in latest_chamber:
            continue

        # Last-name match: exact or hyphen-aware (e.g., "mccormick" in "cherfilus mccormick")
        last_match = (
            cand_last == target_last
            or target_last in cand_tokens
            or cand_last in target_tokens
        )
        if not last_match:
            continue

        # First-name overlap: any token shares 3+ char prefix
        first_match = False
        if not target_firsts or not cand_firsts:
            first_match = True  # no first names to disagree on
        else:
            for tf in target_firsts:
                for cf in cand_firsts:
                    if tf == cf or tf.startswith(cf[:3]) or cf.startswith(tf[:3]):
                        first_match = True
                        break
                if first_match:
                    break
        if not first_match:
            continue

        # Score: prefer exact last-name + first-name match; district tiebreak
        score = 0
        if cand_last == target_last:
            score += 10
        if district and str(m.get("district")) == str(district):
            score += 100
        candidates.append((score, m.get("bioguideId")))

    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def fetch_sponsored_legislation(bioguide: str) -> list[dict]:
    """Fetch all sponsored bills for current congress."""
    bills: list[dict] = []
    offset = 0
    while True:
        d = http_get(
            f"{CONGRESS_BASE}/member/{bioguide}/sponsored-legislation",
            params={"api_key": CONGRESS_KEY, "format": "json", "limit": 250, "offset": offset},
        )
        time.sleep(CONGRESS_DELAY)
        if not d:
            break
        items = d.get("sponsoredLegislation", [])
        if not items:
            break
        # Filter to current congress only
        for b in items:
            if b.get("congress") == CURRENT_CONGRESS:
                bills.append(b)
        # Stop when we drop below current congress (returned in reverse-chronological)
        if all(b.get("congress", 0) < CURRENT_CONGRESS for b in items):
            break
        nxt = d.get("pagination", {}).get("next")
        if not nxt:
            break
        offset += 250
        if offset > 1500:
            break
    return bills


def count_bills_passed(bills: list[dict]) -> int:
    """Count bills that became Public Law."""
    count = 0
    for b in bills:
        text = ((b.get("latestAction") or {}).get("text") or "").lower()
        if "became public law" in text or "public law no" in text or text.startswith("signed by president"):
            count += 1
    return count


_fl_govtrack_roles_cache: list[dict] | None = None


def fetch_fl_govtrack_roles() -> list[dict]:
    """Return current FL congressional roles from GovTrack (includes person.bioguideid)."""
    global _fl_govtrack_roles_cache
    if _fl_govtrack_roles_cache is not None:
        return _fl_govtrack_roles_cache
    d = http_get(f"{GOVTRACK_BASE}/role", params={"current": "true", "state": "FL", "limit": 100})
    time.sleep(GOVTRACK_DELAY)
    roles = d.get("objects", []) if d else []
    _fl_govtrack_roles_cache = roles
    LOG.info("Cached %d FL GovTrack current roles", len(roles))
    return roles


def govtrack_id_for_bioguide(bioguide: str) -> int | None:
    for role in fetch_fl_govtrack_roles():
        person = role.get("person") or {}
        if person.get("bioguideid") == bioguide:
            link = person.get("link", "")
            m = re.search(r"/(\d+)$", link)
            if m:
                return int(m.group(1))
    return None


def compute_attendance(govtrack_id: int) -> tuple[float | None, int, int]:
    """Return (attendance_pct, missed, total) since 119th Congress start."""
    d = http_get(
        f"{GOVTRACK_BASE}/vote_voter",
        params={"person": govtrack_id, "created__gte": GOVTRACK_119_START, "limit": 1},
    )
    time.sleep(GOVTRACK_DELAY)
    if not d or "meta" not in d:
        return None, 0, 0
    total_cast = d["meta"]["total_count"]
    d2 = http_get(
        f"{GOVTRACK_BASE}/vote_voter",
        params={
            "person": govtrack_id,
            "created__gte": GOVTRACK_119_START,
            "option__key": "0",
            "limit": 1,
        },
    )
    time.sleep(GOVTRACK_DELAY)
    if not d2 or "meta" not in d2:
        return None, 0, total_cast
    missed = d2["meta"]["total_count"]
    total_recorded = total_cast + missed  # cast records exclude missing in some govtrack data, so add
    # Actually govtrack vote_voter includes Not Voting records under same query - so total_cast already
    # contains both voted-and-missed. Recompute:
    total_recorded = total_cast
    if total_recorded == 0:
        return None, missed, 0
    pct = (1.0 - missed / total_recorded) * 100.0
    return round(pct, 1), missed, total_recorded


def compute_party_line(govtrack_id: int, member_party: str, sample: int = 30) -> tuple[float | None, int, int]:
    """Sample recent votes; compute % the member voted same as their party's caucus majority.

    Approach: Pull `sample` most recent vote_voter records (excluding Not Voting).
    For each, fetch /vote/{id} once and compute member's party position from
    `majority_party_percent_plus` if member is in chamber's majority party,
    or 1 - that value if minority. Conservative approximation."""
    d = http_get(
        f"{GOVTRACK_BASE}/vote_voter",
        params={
            "person": govtrack_id,
            "created__gte": GOVTRACK_119_START,
            "limit": sample,
            "order_by": "-created",
        },
    )
    time.sleep(GOVTRACK_DELAY)
    if not d:
        return None, 0, 0
    objs = d.get("objects", [])
    if not objs:
        return None, 0, 0

    member_party_norm = (member_party or "").lower()
    is_member_dem = "democrat" in member_party_norm
    is_member_rep = "republican" in member_party_norm
    if not (is_member_dem or is_member_rep):
        return None, 0, 0

    with_party = 0
    counted = 0
    for vv in objs:
        opt = vv.get("option") or {}
        opt_key = opt.get("key")
        if opt_key not in ("+", "-"):  # skip Not Voting / Present
            continue
        member_voted_yes = opt_key == "+"
        vote_id = opt.get("vote")
        if not vote_id:
            continue
        vd = http_get(f"{GOVTRACK_BASE}/vote/{vote_id}")
        time.sleep(GOVTRACK_DELAY)
        if not vd:
            continue
        # Determine majority party of chamber for current congress: 119th = R majority both chambers
        # majority_party_percent_plus = % of majority party (R) voting Yes
        maj_pct_yes = vd.get("majority_party_percent_plus")
        if maj_pct_yes is None:
            continue
        # majority party voted "Yes" if maj_pct_yes > 0.5
        majority_party_yes = maj_pct_yes > 0.5
        # If member is Republican (= majority party), they're "with party" if their yes/no matches
        # If member is Democrat (= minority party), approximate: dems usually opposite of reps on partisan votes
        if is_member_rep:
            with_party += 1 if member_voted_yes == majority_party_yes else 0
        else:
            with_party += 1 if member_voted_yes != majority_party_yes else 0
        counted += 1

    if counted == 0:
        return None, 0, 0
    pct = (with_party / counted) * 100.0
    return round(pct, 1), with_party, counted


_committee_data_cache: tuple[set[str], dict[str, list[str]]] | None = None


def fetch_committee_data() -> tuple[set[str], dict[str, list[str]]]:
    """Return (top_level_codes, bioguide -> [committee_codes])."""
    global _committee_data_cache
    if _committee_data_cache is not None:
        return _committee_data_cache
    top_d = http_get(USDS_COMMITTEES)
    mem_d = http_get(USDS_MEMBERSHIP)
    if not top_d or not mem_d:
        LOG.error("Failed to fetch committee data")
        return set(), {}
    top_codes = {c["thomas_id"] for c in top_d if "thomas_id" in c}
    member_committees: dict[str, list[str]] = defaultdict(list)
    for code, members in mem_d.items():
        if code not in top_codes:
            continue
        for m in members:
            bg = m.get("bioguide")
            if bg:
                member_committees[bg].append(code)
    _committee_data_cache = (top_codes, member_committees)
    LOG.info("Cached committee data: %d top-level committees", len(top_codes))
    return _committee_data_cache


# --- State: OpenStates bulk CSV (no API quota) ----------------------------

OPENSTATES_DATA_DIR = "data/openstates_fl_2025/FL/2025"
_csv_index_cache: dict | None = None


def _load_csv_indexes() -> dict:
    """Build in-memory indexes from FL_2025 OpenStates bulk CSV.

    Returns dict with:
      bills: bill_id -> {chamber, identifier}
      signed_bill_ids: set of bill_ids with executive-signature action
      persons: list of {person_id, last_name, primary_chamber, primary_bill_ids}
    """
    global _csv_index_cache
    if _csv_index_cache is not None:
        return _csv_index_cache
    import csv as _csv
    from collections import Counter as _Counter, defaultdict as _dd

    bills: dict[str, dict] = {}
    with open(f"{OPENSTATES_DATA_DIR}/FL_2025_bills.csv", "r", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            bills[row["id"]] = {
                "chamber": row["organization_classification"],
                "identifier": row["identifier"],
            }

    signed: set[str] = set()
    with open(f"{OPENSTATES_DATA_DIR}/FL_2025_bill_actions.csv", "r", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            if "executive-signature" in row["classification"]:
                signed.add(row["bill_id"])

    person_data: dict[str, dict] = _dd(
        lambda: {"name": "", "chambers": _Counter(), "primary_bill_ids": []}
    )
    with open(f"{OPENSTATES_DATA_DIR}/FL_2025_bill_sponsorships.csv", "r", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            if row["entity_type"] != "person":
                continue
            pid = row["person_id"]
            if not pid:
                continue
            person_data[pid]["name"] = row["name"]
            bill = bills.get(row["bill_id"])
            if not bill:
                continue
            person_data[pid]["chambers"][bill["chamber"]] += 1
            if row["primary"] == "True":
                person_data[pid]["primary_bill_ids"].append(row["bill_id"])

    persons: list[dict] = []
    for pid, d in person_data.items():
        primary_chamber = d["chambers"].most_common(1)[0][0] if d["chambers"] else None
        persons.append(
            {
                "person_id": pid,
                "last_name": d["name"],
                "primary_chamber": primary_chamber,
                "primary_bill_ids": d["primary_bill_ids"],
            }
        )

    _csv_index_cache = {"bills": bills, "signed_bill_ids": signed, "persons": persons}
    LOG.info(
        "Loaded OpenStates CSV indexes: %d bills, %d signed, %d persons",
        len(bills),
        len(signed),
        len(persons),
    )
    return _csv_index_cache


def find_state_person_csv(name: str, chamber: str) -> dict | None:
    """Match an elected_official to a CSV person record by last name + chamber.

    Returns the person dict from _load_csv_indexes()['persons'] or None.
    """
    idx = _load_csv_indexes()
    last_target = _name_tokens(name)[-1] if _name_tokens(name) else ""
    if not last_target:
        return None
    matches = [
        p
        for p in idx["persons"]
        if _name_tokens(p["last_name"]) and _name_tokens(p["last_name"])[-1] == last_target
        and p["primary_chamber"] == chamber
    ]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        # Disambiguate by primary-bill volume — extremely rare for two chamber-mates to share last name
        matches.sort(key=lambda p: len(p["primary_bill_ids"]), reverse=True)
        return matches[0]
    return None


def find_openstates_person(name: str, chamber: str, district: str | None) -> str | None:
    """Look up FL state legislator. chamber: 'upper' or 'lower'."""
    d = http_get(
        f"{OPENSTATES_BASE}/people",
        params={"jurisdiction": "fl", "name": name, "apikey": OPENSTATES_KEY},
    )
    time.sleep(OPENSTATES_DELAY)
    if not d:
        return None
    results = d.get("results", [])
    if not results:
        return None
    norm_name = re.sub(r"[^a-z]", "", name.lower())
    # Prefer current_role match by chamber + district
    for p in results:
        role = p.get("current_role") or {}
        if role.get("org_classification") != chamber:
            continue
        if district and str(role.get("district")) != str(district):
            continue
        return p["id"]
    # Fallback: chamber-only with closest name match
    for p in results:
        role = p.get("current_role") or {}
        if role.get("org_classification") == chamber:
            cand_norm = re.sub(r"[^a-z]", "", p.get("name", "").lower())
            if cand_norm == norm_name:
                return p["id"]
    # Last fallback: any chamber match
    for p in results:
        role = p.get("current_role") or {}
        if role.get("org_classification") == chamber:
            return p["id"]
    return None


def fetch_state_bills(person_id: str) -> list[dict]:
    """Fetch sponsored bills across recent FL sessions."""
    bills: list[dict] = []
    for sess in STATE_SESSIONS:
        page = 1
        while True:
            d = http_get(
                f"{OPENSTATES_BASE}/bills",
                params={
                    "jurisdiction": "fl",
                    "sponsor": person_id,
                    "session": sess,
                    "page": page,
                    "per_page": 20,
                    "apikey": OPENSTATES_KEY,
                },
            )
            time.sleep(OPENSTATES_DELAY)
            if not d:
                break
            results = d.get("results", [])
            bills.extend(results)
            pag = d.get("pagination") or {}
            if page >= (pag.get("max_page") or 1):
                break
            page += 1
            if page > 3:
                break
    return bills


def count_state_bills_passed(bills: list[dict]) -> int:
    """Count bills with action signaling enactment."""
    count = 0
    for b in bills:
        desc = (b.get("latest_action_description") or "").lower()
        if "approved by governor" in desc or "ch. 20" in desc or "chapter no" in desc or "became law" in desc:
            count += 1
    return count


# --- Persistence -----------------------------------------------------------


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


# --- Per-official processing -----------------------------------------------


def process_federal(supabase, off: dict) -> int:
    """Return number of metrics filled."""
    name = off["name"]
    oid = off["id"]
    title = off.get("title") or ""
    chamber = "senate" if "Senator" in title else "house"

    bioguide = find_bioguide(name, chamber, off.get("district"))
    if not bioguide:
        LOG.warning("No bioguide match for %s (%s)", name, title)
        log_failure(supabase, oid, name, "No bioguideId match in Congress.gov FL members")
        return 0

    filled = 0
    # bills_sponsored & bills_passed
    bills = fetch_sponsored_legislation(bioguide)
    sponsored_count = len(bills)
    rating, bench, bench_label = rate_bills_sponsored(sponsored_count, chamber)
    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="bills_sponsored",
        metric_label="Bills Sponsored",
        metric_value=str(sponsored_count),
        metric_unit="bills",
        benchmark_value=str(bench),
        benchmark_label=bench_label,
        performance_rating=rating,
        year=CURRENT_FED_YEAR,
        source="Congress.gov API",
        source_url=f"https://www.congress.gov/member/{bioguide}",
        notes=f"Bills sponsored in {CURRENT_CONGRESS}th Congress",
    )
    filled += 1

    passed = count_bills_passed(bills)
    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="bills_passed",
        metric_label="Bills Signed Into Law",
        metric_value=str(passed),
        metric_unit="bills",
        performance_rating=rate_bills_passed(passed),
        year=CURRENT_FED_YEAR,
        source="Congress.gov API",
        source_url=f"https://www.congress.gov/member/{bioguide}",
        notes=f"Bills enacted as Public Law during {CURRENT_CONGRESS}th Congress",
    )
    filled += 1

    # committee_assignments
    _, member_committees = fetch_committee_data()
    committees = member_committees.get(bioguide, [])
    n_comm = len(committees)
    comm_rating = "good" if n_comm >= 3 else ("meeting" if n_comm >= 1 else "no_data")
    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="committee_assignments",
        metric_label="Committee Assignments",
        metric_value=str(n_comm),
        metric_unit="committees",
        performance_rating=comm_rating,
        year=CURRENT_FED_YEAR,
        source="unitedstates/congress-legislators",
        source_url="https://github.com/unitedstates/congress-legislators",
        notes=f"Top-level committee memberships: {', '.join(committees) or 'none'}",
    )
    filled += 1

    # voting_attendance & party_line via GovTrack
    gt_id = govtrack_id_for_bioguide(bioguide)
    if gt_id:
        att_pct, missed, total = compute_attendance(gt_id)
        if att_pct is not None and total > 0:
            upsert_metric(
                supabase,
                oid,
                name,
                metric_key="voting_attendance",
                metric_label="Voting Attendance",
                metric_value=str(att_pct),
                metric_unit="%",
                benchmark_value="97.0",
                benchmark_label="Chamber median ~97%",
                performance_rating=rate_attendance(att_pct),
                year=CURRENT_FED_YEAR,
                source="GovTrack.us",
                source_url=f"https://www.govtrack.us/congress/members/{gt_id}",
                notes=f"Voted on {total - missed} of {total} roll-call votes since {GOVTRACK_119_START}",
            )
            filled += 1

        party = off.get("party") or ""
        pl_pct, with_party, counted = compute_party_line(gt_id, party, sample=25)
        if pl_pct is not None and counted > 0:
            upsert_metric(
                supabase,
                oid,
                name,
                metric_key="party_line_voting",
                metric_label="Party-Line Voting %",
                metric_value=str(pl_pct),
                metric_unit="%",
                benchmark_value="88.0",
                benchmark_label="Chamber typical 80-92%",
                performance_rating=rate_party_line(pl_pct),
                year=CURRENT_FED_YEAR,
                source="GovTrack.us (sampled)",
                source_url=f"https://www.govtrack.us/congress/members/{gt_id}",
                notes=f"Estimated from {counted} recent votes; voted with party on {with_party}",
            )
            filled += 1
    else:
        LOG.warning("No GovTrack id for %s (%s)", name, bioguide)

    return filled


def process_state(supabase, off: dict) -> int:
    name = off["name"]
    oid = off["id"]
    title = off.get("title") or ""
    district = off.get("district")
    chamber = "upper" if "Senator" in title else "lower"

    idx = _load_csv_indexes()
    person = find_state_person_csv(name, chamber)
    if not person:
        LOG.warning("No CSV match for %s (%s, district %s)", name, title, district)
        log_failure(
            supabase,
            oid,
            name,
            f"No OpenStates CSV person for chamber={chamber} district={district}",
        )
        return 0

    primary_bill_ids = person["primary_bill_ids"]
    sponsored_count = len(primary_bill_ids)
    passed_count = sum(1 for bid in primary_bill_ids if bid in idx["signed_bill_ids"])
    person_slug = person["person_id"].split("/")[-1]
    src_url = f"https://openstates.org/person/{person_slug}/"

    filled = 0
    rating, bench, bench_label = rate_state_bills_sponsored(sponsored_count, chamber)
    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="bills_sponsored",
        metric_label="Bills Sponsored",
        metric_value=str(sponsored_count),
        metric_unit="bills",
        benchmark_value=str(bench),
        benchmark_label=bench_label,
        performance_rating=rating,
        year=CURRENT_STATE_YEAR,
        source="OpenStates bulk data (FL 2025 session)",
        source_url=src_url,
        notes=f"Primary-sponsored bills filed in FL 2025 regular session",
    )
    filled += 1

    upsert_metric(
        supabase,
        oid,
        name,
        metric_key="bills_passed",
        metric_label="Bills Signed Into Law",
        metric_value=str(passed_count),
        metric_unit="bills",
        performance_rating=rate_bills_passed(passed_count),
        year=CURRENT_STATE_YEAR,
        source="OpenStates bulk data (FL 2025 session)",
        source_url=src_url,
        notes="Primary-sponsored bills with executive-signature action",
    )
    filled += 1

    return filled


# --- Driver ----------------------------------------------------------------


def fetch_officials(supabase, scope: str) -> list[dict]:
    """Pull FL legislators from elected_officials."""
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, level, state, district, party")
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
        title = o.get("title") or ""
        if scope in ("federal", "all") and o.get("level") == "federal":
            if "U.S. Senator" in title or "U.S. Representative" in title:
                out.append(o)
        if scope in ("state", "all"):
            # State legislators by title, ignore federal
            if o.get("level") == "federal":
                continue
            if re.match(r"^(State Senator|Senator($|,))", title) or re.match(
                r"^(State Representative|Representative($|,))", title
            ):
                out.append(o)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=("federal", "state", "all"), default="all")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N officials")
    parser.add_argument("--ids", type=str, default=None, help="Comma-separated official_ids only")
    args = parser.parse_args()

    if not (CONGRESS_KEY and OPENSTATES_KEY and SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing required env vars")
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    officials = fetch_officials(supabase, args.only)
    if args.ids:
        wanted = {int(x) for x in args.ids.split(",")}
        officials = [o for o in officials if o["id"] in wanted]
    if args.limit:
        officials = officials[: args.limit]

    LOG.info(
        "Processing %d officials (%s)",
        len(officials),
        args.only,
    )

    total_filled = 0
    failures = 0
    processed = 0
    for off in officials:
        processed += 1
        try:
            if off.get("level") == "federal":
                n = process_federal(supabase, off)
            else:
                n = process_state(supabase, off)
            total_filled += n
            LOG.info("[%d/%d] Filled %d metrics for %s", processed, len(officials), n, off["name"])
        except Exception as e:
            failures += 1
            LOG.exception("Failed for %s: %s", off.get("name"), e)
            log_failure(supabase, off["id"], off.get("name", ""), f"Unhandled exception: {e}")

        if processed % 20 == 0:
            LOG.info(
                "Progress: %d/%d processed | %d metrics filled | %d failures",
                processed,
                len(officials),
                total_filled,
                failures,
            )

    LOG.info(
        "DONE: %d processed | %d metrics upserted | %d failures",
        processed,
        total_filled,
        failures,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
