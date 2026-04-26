"""Session 7 ingestion: drill-down legislative_activity records.

Populates legislative_activity with one row per *individual* record so the
frontend scorecard cards can drill into the underlying data:

  bill_sponsored      one row per bill the official primary-sponsored
  bill_cosponsored    one row per cosponsored bill (federal only)
  vote                one row per roll-call vote the official cast
  committee           one row per committee assignment

Phases (run independently with --phase, default 'all'):

  federal-bills       Congress.gov sponsored + cosponsored legislation
  federal-committees  unitedstates/congress-legislators committee membership
  federal-votes       GovTrack vote_voter (capped per --vote-cap, default 250)
  state-bills         OpenStates bulk CSV (FL_2025_bills + sponsorships)
  state-committees    OpenStates v3 /people?include=memberships (1 call per legislator)
  state-votes         OpenStates bulk CSV (FL_2025_votes + vote_people)

Idempotency: backed by the idx_legislative_unique partial-coalesce index;
inserts use ON CONFLICT DO NOTHING via Supabase REST upsert with
ignore_duplicates=True.

Pacing & quotas
  Congress.gov   1.1s/req  (~3500/hr soft limit)
  GovTrack       1.1s/req  (no published quota, polite)
  OpenStates     1.5s/req  (committees only — bulk CSV avoids API for votes/bills)

Usage
  python scripts/ingest_legislative_detail.py --phase all
  python scripts/ingest_legislative_detail.py --phase federal-bills --limit 2
  python scripts/ingest_legislative_detail.py --phase state-votes --ids 1234,5678
  python scripts/ingest_legislative_detail.py --phase federal-votes --vote-cap 100
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
from typing import Any, Iterable

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Reuse helpers from Session 1 — they handle FL-member caching, name matching,
# committee data, and OpenStates bulk CSV loading.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingest_congress_metrics import (  # noqa: E402
    CONGRESS_BASE,
    CONGRESS_DELAY,
    CONGRESS_KEY,
    CURRENT_CONGRESS,
    GOVTRACK_BASE,
    GOVTRACK_DELAY,
    GOVTRACK_119_START,
    OPENSTATES_BASE,
    OPENSTATES_DELAY,
    OPENSTATES_DATA_DIR,
    OPENSTATES_KEY,
    SUPABASE_KEY,
    SUPABASE_URL,
    USDS_COMMITTEES,
    _load_csv_indexes,
    fetch_committee_data,
    fetch_fl_congress_members,
    fetch_fl_govtrack_roles,
    find_bioguide,
    find_state_person_csv,
    govtrack_id_for_bioguide,
    http_get,
)

LOG = logging.getLogger("legislative_detail")
if not LOG.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(h)
    fh = logging.FileHandler("ingest_legislative_detail.log", mode="a")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(fh)
    LOG.setLevel(logging.INFO)


# --- Status & vote-position normalizers -----------------------------------


_FED_STATUS_RULES = (
    ("became public law", "enacted"),
    ("signed by president", "enacted"),
    ("public law no", "enacted"),
    ("vetoed", "vetoed"),
    ("passed senate", "passed_chamber"),
    ("passed house", "passed_chamber"),
    ("passed/agreed to", "passed_chamber"),
    ("agreed to in senate", "passed_chamber"),
    ("agreed to in house", "passed_chamber"),
    ("reported by", "in_committee_reported"),
    ("reported to", "in_committee_reported"),
    ("placed on", "in_committee_reported"),
    ("referred to", "in_committee"),
    ("committee consideration", "in_committee"),
)


def federal_status_from_action(action_text: str | None) -> str:
    s = (action_text or "").lower()
    for needle, status in _FED_STATUS_RULES:
        if needle in s:
            return status
    return "introduced"


_STATE_ACTION_RANK = {
    "executive-signature": (90, "enacted"),
    "veto-override-passage": (80, "passed_chamber"),
    "passage": (60, "passed_chamber"),
    "amendment-passage": (50, "passed_chamber"),
    "committee-passage": (40, "in_committee_reported"),
    "committee-passage-favorable": (40, "in_committee_reported"),
    "withdrawal": (30, "withdrawn"),
    "failure": (30, "failed"),
    "veto": (30, "vetoed"),
    "filing": (10, "introduced"),
    "introduction": (10, "introduced"),
    "referral-committee": (10, "in_committee"),
    "reading-1": (10, "introduced"),
}


def _vote_position_label(option_key: str) -> str | None:
    """GovTrack option keys: + (Yea), - (Nay), P (Present), 0 (Not Voting)."""
    return {
        "+": "Yea",
        "-": "Nay",
        "P": "Present",
        "0": "Not Voting",
    }.get(option_key)


def _state_position_label(option: str) -> str | None:
    if not option:
        return None
    o = option.lower().strip()
    if o == "yes":
        return "Yea"
    if o == "no":
        return "Nay"
    if o in ("absent", "excused"):
        return "Absent"
    if o in ("not voting", "abstain"):
        return "Not Voting"
    if o in ("present", "other"):
        return "Present"
    return option.title()


def _format_fed_bill_number(b: dict) -> tuple[str, str]:
    """Return (display, slug) for Congress.gov bill record.

    display: 'H.R. 1234' style.
    slug:    'house-bill/1234' style for URL composition.
    """
    bt = (b.get("type") or "").upper()
    num = b.get("number") or ""
    code_map = {
        "HR": "H.R.",
        "S": "S.",
        "HJRES": "H.J.Res.",
        "SJRES": "S.J.Res.",
        "HCONRES": "H.Con.Res.",
        "SCONRES": "S.Con.Res.",
        "HRES": "H.Res.",
        "SRES": "S.Res.",
    }
    slug_map = {
        "HR": "house-bill",
        "S": "senate-bill",
        "HJRES": "house-joint-resolution",
        "SJRES": "senate-joint-resolution",
        "HCONRES": "house-concurrent-resolution",
        "SCONRES": "senate-concurrent-resolution",
        "HRES": "house-resolution",
        "SRES": "senate-resolution",
    }
    display = f"{code_map.get(bt, bt)} {num}".strip() if num else (code_map.get(bt) or bt)
    slug = f"{slug_map.get(bt, bt.lower())}/{num}" if num else (slug_map.get(bt, bt.lower()) or bt.lower())
    return display, slug


# --- Persistence -----------------------------------------------------------


def _row(
    *,
    official_name: str,
    official_level: str,
    chamber: str | None,
    activity_type: str,
    title: str,
    bill_number: str | None = None,
    description: str | None = None,
    status: str | None = None,
    vote_position: str | None = None,
    date: str | None = None,
    source: str,
    source_url: str | None = None,
    state: str = "FL",
) -> dict:
    return {
        "official_name": official_name,
        "official_level": official_level,
        "chamber": chamber,
        "activity_type": activity_type,
        "bill_number": bill_number,
        "title": (title or "").strip()[:1000] or "(untitled)",
        "description": (description or None) and description[:2000],
        "status": status,
        "vote_position": vote_position,
        "date": date,
        "source": source,
        "source_url": source_url,
        "state": state,
    }


def insert_rows(supabase, rows: list[dict]) -> int:
    """Bulk insert with ON CONFLICT DO NOTHING via PostgREST.

    Uses upsert(ignore_duplicates=True, on_conflict=<index columns>) which maps
    to PostgREST's `Prefer: resolution=ignore-duplicates` for the named
    constraint. Falls back to per-row insert on bulk failure.
    """
    if not rows:
        return 0
    try:
        # The named constraint must match the unique index column list.
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
                LOG.warning("Row insert failed for %s/%s: %s", r.get("official_name"), r.get("bill_number"), e2)
        return ok


def _chunked(items: list[dict], n: int = 200) -> Iterable[list[dict]]:
    for i in range(0, len(items), n):
        yield items[i:i + n]


# --- Federal: bills (sponsored + cosponsored) ------------------------------


def _fetch_member_bills(bioguide: str, kind: str, max_pages: int = 10) -> list[dict]:
    """kind ∈ {'sponsored','cosponsored'}."""
    path = "sponsored-legislation" if kind == "sponsored" else "cosponsored-legislation"
    field = "sponsoredLegislation" if kind == "sponsored" else "cosponsoredLegislation"
    bills: list[dict] = []
    offset = 0
    for _ in range(max_pages):
        d = http_get(
            f"{CONGRESS_BASE}/member/{bioguide}/{path}",
            params={"api_key": CONGRESS_KEY, "format": "json", "limit": 250, "offset": offset},
        )
        time.sleep(CONGRESS_DELAY)
        if not d:
            break
        items = d.get(field) or []
        if not items:
            break
        # Filter to current congress (oldest pages will leave it).
        cur = [b for b in items if b.get("congress") == CURRENT_CONGRESS]
        bills.extend(cur)
        if all(b.get("congress", 0) < CURRENT_CONGRESS for b in items):
            break
        if len(items) < 250:
            break
        offset += 250
    return bills


def process_federal_bills(supabase, off: dict, bioguide: str) -> tuple[int, int]:
    """Returns (sponsored_inserted, cosponsored_inserted)."""
    name = off["name"]
    chamber = "senate" if "Senator" in (off.get("title") or "") else "house"

    rows: list[dict] = []
    for kind in ("sponsored", "cosponsored"):
        bills = _fetch_member_bills(bioguide, kind)
        activity = "bill_sponsored" if kind == "sponsored" else "bill_cosponsored"
        for b in bills:
            display_no, slug = _format_fed_bill_number(b)
            action = b.get("latestAction") or {}
            url = b.get("url") or f"https://www.congress.gov/bill/{CURRENT_CONGRESS}th-congress/{slug}"
            rows.append(_row(
                official_name=name,
                official_level="federal",
                chamber=chamber,
                activity_type=activity,
                bill_number=display_no or None,
                title=b.get("title") or display_no or "(untitled)",
                description=action.get("text"),
                status=federal_status_from_action(action.get("text")),
                date=action.get("actionDate"),
                source="Congress.gov",
                source_url=url,
            ))

    # Split for clearer counters.
    spon = [r for r in rows if r["activity_type"] == "bill_sponsored"]
    cospon = [r for r in rows if r["activity_type"] == "bill_cosponsored"]
    s_ok = c_ok = 0
    for chunk in _chunked(spon):
        s_ok += insert_rows(supabase, chunk)
    for chunk in _chunked(cospon):
        c_ok += insert_rows(supabase, chunk)
    return s_ok, c_ok


# --- Federal: committees ---------------------------------------------------


_committee_meta_cache: dict[str, dict] | None = None


def _committee_meta() -> dict[str, dict]:
    """Map thomas_id -> {name, chamber, url, subcommittees:[{thomas_id,name}]}."""
    global _committee_meta_cache
    if _committee_meta_cache is not None:
        return _committee_meta_cache
    d = http_get(USDS_COMMITTEES)
    out: dict[str, dict] = {}
    if not d:
        _committee_meta_cache = out
        return out
    for c in d:
        tid = c.get("thomas_id")
        if not tid:
            continue
        out[tid] = {
            "name": c.get("name", tid),
            "chamber": (c.get("type") or "").lower() or "joint",
            "url": c.get("url"),
            "jurisdiction": c.get("jurisdiction"),
            "subcommittees": {
                f"{tid}{sc.get('thomas_id','')}": sc.get("name")
                for sc in (c.get("subcommittees") or [])
            },
        }
    _committee_meta_cache = out
    return out


def _committee_membership_full() -> dict[str, list[dict]]:
    """bioguide -> [{code, name, role, parent_name?}]. Includes subcommittees."""
    d = http_get("https://unitedstates.github.io/congress-legislators/committee-membership-current.json")
    if not d:
        return {}
    meta = _committee_meta()
    out: dict[str, list[dict]] = defaultdict(list)
    for code, members in d.items():
        if code in meta:
            entry_name = meta[code]["name"]
            parent = None
        else:
            # Subcommittee: find parent by prefix
            parent_code = code[:4] if len(code) >= 4 else None
            if not parent_code or parent_code not in meta:
                continue
            entry_name = meta[parent_code]["subcommittees"].get(code) or code
            parent = meta[parent_code]["name"]
        for m in members:
            bg = m.get("bioguide")
            if not bg:
                continue
            role = (m.get("title") or m.get("party") or "Member")
            if role.lower() in ("majority", "minority"):
                role = "Member"
            out[bg].append({"code": code, "name": entry_name, "role": role, "parent": parent})
    return out


def process_federal_committees(supabase, off: dict, bioguide: str) -> int:
    name = off["name"]
    chamber = "senate" if "Senator" in (off.get("title") or "") else "house"
    membership = _committee_membership_full()
    entries = membership.get(bioguide, [])
    rows = []
    meta = _committee_meta()
    for e in entries:
        cmeta = meta.get(e["code"][:4] if e.get("parent") else e["code"], {})
        url = cmeta.get("url")
        title = e["name"]
        if e.get("parent"):
            title = f"{e['parent']} → {title}"
        rows.append(_row(
            official_name=name,
            official_level="federal",
            chamber=chamber,
            activity_type="committee",
            bill_number=e["code"],
            title=title,
            description=f"{e['role']} ({chamber.title()})",
            source="unitedstates/congress-legislators",
            source_url=url or "https://github.com/unitedstates/congress-legislators",
        ))
    return insert_rows(supabase, rows)


# --- Federal: votes --------------------------------------------------------


_govtrack_vote_cache: dict[int, dict] = {}


def _fetch_govtrack_vote(vote_id: int) -> dict | None:
    if vote_id in _govtrack_vote_cache:
        return _govtrack_vote_cache[vote_id]
    d = http_get(f"{GOVTRACK_BASE}/vote/{vote_id}")
    time.sleep(GOVTRACK_DELAY)
    _govtrack_vote_cache[vote_id] = d or {}
    return d


def _vote_voter_pages(govtrack_id: int, cap: int) -> list[dict]:
    """Pull most-recent `cap` votes for the given GovTrack person."""
    out: list[dict] = []
    page_size = min(cap, 200)
    offset = 0
    while len(out) < cap:
        d = http_get(
            f"{GOVTRACK_BASE}/vote_voter",
            params={
                "person": govtrack_id,
                "created__gte": GOVTRACK_119_START,
                "limit": page_size,
                "offset": offset,
                "order_by": "-created",
            },
        )
        time.sleep(GOVTRACK_DELAY)
        if not d:
            break
        objs = d.get("objects") or []
        if not objs:
            break
        out.extend(objs)
        if len(objs) < page_size:
            break
        offset += page_size
    return out[:cap]


def process_federal_votes(supabase, off: dict, govtrack_id: int, cap: int) -> int:
    name = off["name"]
    chamber = "senate" if "Senator" in (off.get("title") or "") else "house"
    voter_records = _vote_voter_pages(govtrack_id, cap)
    rows: list[dict] = []
    for vv in voter_records:
        opt = vv.get("option") or {}
        position = _vote_position_label(opt.get("key", ""))
        if not position:
            continue
        vote_id = opt.get("vote")
        if not vote_id:
            continue
        vd = _fetch_govtrack_vote(int(vote_id))
        if not vd:
            continue
        related = vd.get("related_bill") or {}
        bill_label = related.get("display_number") or (
            f"{related.get('bill_type_label','')} {related.get('number','')}".strip()
            if related else ""
        )
        question = vd.get("question") or vd.get("category") or "Roll-call vote"
        url = vd.get("link") or f"https://www.govtrack.us/congress/votes/{vd.get('congress','')}-{vd.get('chamber','')}/{vd.get('number','')}"
        # Status reflects the chamber outcome of the vote, not the bill.
        outcome = (vd.get("result") or "").lower()
        if "pass" in outcome:
            status = "passed_chamber"
        elif "fail" in outcome or "reject" in outcome:
            status = "failed"
        else:
            status = None
        rows.append(_row(
            official_name=name,
            official_level="federal",
            chamber=chamber,
            activity_type="vote",
            bill_number=bill_label or None,
            title=question[:500],
            description=f"Result: {vd.get('result')}; Total Yea {vd.get('total_plus','?')} / Nay {vd.get('total_minus','?')}",
            status=status,
            vote_position=position,
            date=(vd.get("created") or "")[:10] or None,
            source="GovTrack.us",
            source_url=url,
        ))

    inserted = 0
    for chunk in _chunked(rows):
        inserted += insert_rows(supabase, chunk)
    return inserted


# --- State: bills (bulk CSV) -----------------------------------------------


_state_action_index: dict[str, dict] | None = None


def _build_state_action_index() -> dict[str, dict]:
    """Per-bill latest action with rank-based status mapping."""
    global _state_action_index
    if _state_action_index is not None:
        return _state_action_index
    actions_path = f"{OPENSTATES_DATA_DIR}/FL_2025_bill_actions.csv"
    best: dict[str, tuple[int, str, str, str]] = {}
    with open(actions_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            bid = row["bill_id"]
            classifications = row["classification"] or ""
            description = row.get("description") or ""
            date = row.get("date") or ""
            rank, status = 0, "introduced"
            for cls, (r, s) in _STATE_ACTION_RANK.items():
                if cls in classifications:
                    if r > rank:
                        rank, status = r, s
            existing = best.get(bid)
            if existing is None or rank > existing[0] or (rank == existing[0] and date > existing[3]):
                best[bid] = (rank, status, description, date)
    out = {bid: {"status": st, "description": desc, "date": date} for bid, (_, st, desc, date) in best.items()}
    _state_action_index = out
    return out


def _state_bill_url(identifier: str, session: str = "2025") -> str:
    # FL Senate identifiers are SB/SR/SJR/SM/SCR/SPB; House: HB/HR/HJR/HM/HCR/HPB.
    no_space = identifier.replace(" ", "")
    return f"https://www.flsenate.gov/Session/Bill/{session}/{no_space}"


def process_state_bills(supabase, off: dict) -> int:
    name = off["name"]
    title = off.get("title") or ""
    chamber = "upper" if "Senator" in title else "lower"
    chamber_label = "senate" if chamber == "upper" else "house"

    person = find_state_person_csv(name, chamber)
    if not person:
        LOG.warning("No CSV person for %s (%s)", name, title)
        return 0
    idx = _load_csv_indexes()
    actions = _build_state_action_index()

    rows = []
    for bid in person["primary_bill_ids"]:
        bill = idx["bills"].get(bid)
        if not bill:
            continue
        identifier = bill.get("identifier") or ""
        info = actions.get(bid, {})
        rows.append(_row(
            official_name=name,
            official_level="state",
            chamber=chamber_label,
            activity_type="bill_sponsored",
            bill_number=identifier or None,
            title=identifier or "(untitled)",  # title field reused as identifier; actual subject in description
            description=info.get("description"),
            status=info.get("status") or "introduced",
            date=info.get("date") or None,
            source="OpenStates bulk data (FL 2025 session)",
            source_url=_state_bill_url(identifier),
        ))
    return insert_rows(supabase, rows)


# --- State: committees (OpenStates v3 API) ---------------------------------


_state_committee_index: dict[str, list[dict]] | None = None


def _build_state_committee_index() -> dict[str, list[dict]]:
    """Fetch all FL committees with memberships once; index person_id -> [{name,role,id}].

    OpenStates v3 doesn't expose memberships through /people anymore, so iterate
    /committees and pivot to a per-person view.
    """
    global _state_committee_index
    if _state_committee_index is not None:
        return _state_committee_index
    out: dict[str, list[dict]] = defaultdict(list)
    page = 1
    while True:
        d = http_get(
            f"{OPENSTATES_BASE}/committees",
            params={
                "jurisdiction": "fl",
                "include": "memberships",
                "per_page": 20,
                "page": page,
                "apikey": OPENSTATES_KEY,
            },
        )
        time.sleep(OPENSTATES_DELAY)
        if not d:
            break
        results = d.get("results") or []
        for c in results:
            cname = c.get("name") or "Committee"
            cclass = (c.get("classification") or "committee").lower()
            cid = c.get("id")
            for mem in c.get("memberships") or []:
                person = mem.get("person") or {}
                pid = person.get("id")
                if not pid:
                    continue
                out[pid].append({
                    "name": cname,
                    "role": mem.get("role") or "member",
                    "classification": cclass,
                    "committee_id": cid,
                })
        pag = d.get("pagination") or {}
        if page >= (pag.get("max_page") or 1):
            break
        page += 1
        if page > 20:
            break
    _state_committee_index = out
    LOG.info("Cached FL committee memberships for %d people", len(out))
    return _state_committee_index


def process_state_committees(supabase, off: dict) -> int:
    name = off["name"]
    title = off.get("title") or ""
    chamber = "upper" if "Senator" in title else "lower"
    chamber_label = "senate" if chamber == "upper" else "house"
    person = find_state_person_csv(name, chamber)
    if not person:
        return 0
    pid = person["person_id"]
    cmt_idx = _build_state_committee_index()
    entries = cmt_idx.get(pid, [])
    if not entries:
        return 0
    pid_slug = pid.split("/")[-1]
    rows = []
    for e in entries:
        rows.append(_row(
            official_name=name,
            official_level="state",
            chamber=chamber_label,
            activity_type="committee",
            bill_number=e["committee_id"] or e["name"][:80],
            title=e["name"],
            description=f"{e['role'].title()} (FL {chamber_label.title()}, {e['classification']})",
            source="OpenStates v3",
            source_url=f"https://openstates.org/person/{pid_slug}/",
        ))
    return insert_rows(supabase, rows)


# --- State: votes (bulk CSV) -----------------------------------------------


_state_vote_index: dict[str, list[dict]] | None = None


def _build_state_vote_index() -> dict[str, list[dict]]:
    """person_id -> [{vote_id, bill_id, position, motion, date, result}]."""
    global _state_vote_index
    if _state_vote_index is not None:
        return _state_vote_index
    base = OPENSTATES_DATA_DIR
    votes_meta: dict[str, dict] = {}
    with open(f"{base}/FL_2025_votes.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            votes_meta[row["id"]] = {
                "bill_id": row.get("bill_id") or "",
                "motion_text": row.get("motion_text") or "",
                "result": row.get("result") or "",
                "start_date": (row.get("start_date") or "")[:10],
                "classification": row.get("motion_classification") or "",
            }

    out: dict[str, list[dict]] = defaultdict(list)
    with open(f"{base}/FL_2025_vote_people.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = row.get("voter_id") or ""
            if not pid:
                continue
            ve_id = row["vote_event_id"]
            meta = votes_meta.get(ve_id)
            if not meta:
                continue
            out[pid].append({
                "vote_id": ve_id,
                "bill_id": meta["bill_id"],
                "position": _state_position_label(row["option"]),
                "motion": meta["motion_text"],
                "date": meta["start_date"],
                "result": meta["result"],
                "classification": meta["classification"],
            })
    _state_vote_index = out
    return out


def process_state_votes(supabase, off: dict, cap: int) -> int:
    name = off["name"]
    title = off.get("title") or ""
    chamber = "upper" if "Senator" in title else "lower"
    chamber_label = "senate" if chamber == "upper" else "house"
    person = find_state_person_csv(name, chamber)
    if not person:
        return 0
    idx = _load_csv_indexes()
    vote_idx = _build_state_vote_index()

    person_votes = vote_idx.get(person["person_id"], [])
    # Prefer final-passage votes over procedural; sort by date desc.
    def _rank(v: dict) -> int:
        cls = v.get("classification") or ""
        if "passage" in cls and "committee" not in cls:
            return 3
        if "committee-passage" in cls:
            return 2
        if "amendment" in cls:
            return 1
        return 0
    person_votes.sort(key=lambda v: (_rank(v), v.get("date", "")), reverse=True)
    person_votes = person_votes[:cap]

    rows: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()
    for v in person_votes:
        position = v.get("position")
        if not position:
            continue
        bill = idx["bills"].get(v["bill_id"]) or {}
        identifier = bill.get("identifier") or ""
        # Dedup against the unique index: (name, vote, identifier, motion, position).
        key = (identifier, position)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        result_l = (v.get("result") or "").lower()
        status = "passed_chamber" if "pass" in result_l else ("failed" if "fail" in result_l else None)
        rows.append(_row(
            official_name=name,
            official_level="state",
            chamber=chamber_label,
            activity_type="vote",
            bill_number=identifier or None,
            title=v.get("motion") or "Roll-call vote",
            description=f"FL {chamber_label.title()} vote on {identifier or 'measure'}",
            status=status,
            vote_position=position,
            date=v.get("date") or None,
            source="OpenStates bulk data (FL 2025 session)",
            source_url=_state_bill_url(identifier) if identifier else None,
        ))

    inserted = 0
    for chunk in _chunked(rows):
        inserted += insert_rows(supabase, chunk)
    return inserted


# --- Driver ----------------------------------------------------------------


PHASES = (
    "federal-bills",
    "federal-committees",
    "federal-votes",
    "state-bills",
    "state-committees",
    "state-votes",
    "all",
)


def fetch_officials(supabase, scope: str) -> list[dict]:
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
        level = o.get("level")
        if scope == "federal" and level != "federal":
            continue
        if scope == "state" and level == "federal":
            continue
        if level == "federal":
            if "U.S. Senator" in title or "U.S. Representative" in title:
                out.append(o)
        else:
            if re.match(r"^(State Senator|Senator($|,))", title) or re.match(
                r"^(State Representative|Representative($|,))", title
            ):
                out.append(o)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=PHASES, default="all")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--ids", type=str, default=None)
    parser.add_argument("--vote-cap", type=int, default=250,
                        help="Max votes to ingest per legislator per source")
    args = parser.parse_args()

    if not (CONGRESS_KEY and OPENSTATES_KEY and SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing required env vars")
        return 1
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    phase = args.phase
    needs_federal = phase in ("federal-bills", "federal-committees", "federal-votes", "all")
    needs_state = phase in ("state-bills", "state-committees", "state-votes", "all")

    federal: list[dict] = []
    state: list[dict] = []
    if needs_federal:
        federal = fetch_officials(supabase, "federal")
    if needs_state:
        state = fetch_officials(supabase, "state")

    if args.ids:
        wanted = {int(x) for x in args.ids.split(",")}
        federal = [o for o in federal if o["id"] in wanted]
        state = [o for o in state if o["id"] in wanted]
    if args.limit:
        federal = federal[: args.limit]
        state = state[: args.limit]

    LOG.info("Phase=%s | federal=%d state=%d vote_cap=%d", phase, len(federal), len(state), args.vote_cap)

    counters: dict[str, int] = defaultdict(int)
    failures = 0

    # Federal phases
    if phase in ("federal-bills", "federal-committees", "federal-votes", "all"):
        for i, off in enumerate(federal, 1):
            try:
                title_str = off.get("title") or ""
                fed_chamber = "senate" if "Senator" in title_str else "house"
                bg = find_bioguide(off["name"], fed_chamber, off.get("district"))
                if not bg:
                    LOG.warning("[fed %d/%d] no bioguide for %s", i, len(federal), off["name"])
                    continue
                if phase in ("federal-bills", "all"):
                    s, c = process_federal_bills(supabase, off, bg)
                    counters["bill_sponsored"] += s
                    counters["bill_cosponsored"] += c
                if phase in ("federal-committees", "all"):
                    counters["committee"] += process_federal_committees(supabase, off, bg)
                if phase in ("federal-votes", "all"):
                    gt = govtrack_id_for_bioguide(bg)
                    if gt:
                        counters["vote"] += process_federal_votes(supabase, off, gt, args.vote_cap)
                    else:
                        LOG.warning("[fed %d/%d] no GovTrack id for %s (%s)", i, len(federal), off["name"], bg)
                LOG.info("[fed %d/%d] %s — running totals: %s", i, len(federal), off["name"], dict(counters))
            except Exception as e:
                failures += 1
                LOG.exception("federal failure for %s: %s", off.get("name"), e)
            if i % 5 == 0:
                LOG.info("Federal progress %d/%d | %s | %d failures", i, len(federal), dict(counters), failures)

    # State phases
    if phase in ("state-bills", "state-committees", "state-votes", "all"):
        for i, off in enumerate(state, 1):
            try:
                if phase in ("state-bills", "all"):
                    counters["bill_sponsored"] += process_state_bills(supabase, off)
                if phase in ("state-committees", "all"):
                    counters["committee"] += process_state_committees(supabase, off)
                if phase in ("state-votes", "all"):
                    counters["vote"] += process_state_votes(supabase, off, args.vote_cap)
                if i % 5 == 0:
                    LOG.info("State progress %d/%d | %s | %d failures", i, len(state), dict(counters), failures)
            except Exception as e:
                failures += 1
                LOG.exception("state failure for %s: %s", off.get("name"), e)

    LOG.info("DONE phase=%s totals=%s failures=%d", phase, dict(counters), failures)
    return 0


if __name__ == "__main__":
    sys.exit(main())
