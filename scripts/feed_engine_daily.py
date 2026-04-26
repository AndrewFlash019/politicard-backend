"""Feed Engine v1: Daily ingestion pipeline that brings feed_cards from stale to live.

Phases (run individually with --phase, or all-at-once with --phase all):

  1  Transform existing legislative_activity rows -> feed_cards (idempotent UPSERT on dedup_key)
  2  Smart grouping: collapse 4+ same-group cards into a digest
  3  Snapshot accountability_metrics for week-over-week change detection
  4  Live ingestion: Congress.gov + OpenStates (rotated) + .gov RSS feeds
  5  Daily Brief per FL ZIP (city_zips + county_zips universe)
  6  Coming-up events: Congress + FL Senate/House calendars
  7  Expiry / cleanup of stale cards and old briefs

Run:
  python scripts/feed_engine_daily.py --phase all
  python scripts/feed_engine_daily.py --phase 1
  python scripts/feed_engine_daily.py --phase 5 --zip 32164
  python scripts/feed_engine_daily.py --phase 4 --dry-run

Env: CONGRESS_API_KEY, OPENSTATES_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

LOG = logging.getLogger("feed_engine")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("feed_engine_daily.log", mode="a"),
    ],
)

CONGRESS_KEY = os.getenv("CONGRESS_API_KEY")
OPENSTATES_KEY = os.getenv("OPENSTATES_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

CURRENT_CONGRESS = 119
USER_AGENT = "PolitiScore Feed Engine +https://politiscore.com"

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

# --- Content policy ----------------------------------------------------------

ALLOWED_DOMAINS = {
    "congress.gov",
    "openstates.org",
    "govtrack.us",
    "courtlistener.com",
    "fldoe.org",
    "flsheriffs.org",
    "flgov.com",
    "fldfs.com",
    "dos.fl.gov",
    "myfloridahouse.gov",
    "flsenate.gov",
    "floridarevenue.com",
    "myfloridacfo.com",
    "edr.state.fl.us",
    "fdle.state.fl.us",
}
ALLOWED_SUFFIXES = (".senate.gov", ".house.gov", ".gov", ".gov.us", ".fl.us", ".myflorida.com")
BLOCKED_SUFFIXES = (".substack.com", ".medium.com", ".blogspot.com", ".wordpress.com")
BLOCKED_DOMAINS = {
    "nytimes.com", "miamiherald.com", "tampabay.com", "washingtonpost.com",
    "orlandosentinel.com", "foxnews.com", "cnn.com", "breitbart.com",
    "twitter.com", "x.com", "facebook.com", "reddit.com",
}


def is_allowed_source(url: str | None) -> bool:
    """Return True if URL is from an allow-listed civic/government source."""
    if not url:
        return True  # no URL -> not gating on source
    try:
        host = urlparse(url).hostname or ""
    except ValueError:
        return False
    host = host.lower()
    if not host:
        return False
    if host in ALLOWED_DOMAINS:
        return True
    if any(host == bd or host.endswith("." + bd) for bd in BLOCKED_DOMAINS):
        return False
    if any(host.endswith(suf) for suf in BLOCKED_SUFFIXES):
        return False
    if any(host.endswith(suf) for suf in ALLOWED_SUFFIXES):
        return True
    return False


# --- Persistence helpers ----------------------------------------------------


def upsert_card(supabase, dry_run: bool, **fields) -> tuple[bool, bool]:
    """Insert or update a feed_card via dedup_key. Returns (inserted, updated)."""
    if not fields.get("dedup_key"):
        raise ValueError("upsert_card requires dedup_key")
    if dry_run:
        LOG.debug("DRY upsert_card %s", fields["dedup_key"])
        return False, False

    existing = (
        supabase.table("feed_cards")
        .select("id, body, last_updated_at, update_count")
        .eq("dedup_key", fields["dedup_key"])
        .limit(1)
        .execute()
    )
    now_iso = datetime.now(timezone.utc).isoformat()
    if existing.data:
        row = existing.data[0]
        old_body = row.get("body") or ""
        new_body = fields.get("body") or ""
        if old_body == new_body:
            return False, False  # no-op
        update_payload = {
            **{k: v for k, v in fields.items() if k != "dedup_key"},
            "last_updated_at": now_iso,
            "update_count": (row.get("update_count") or 0) + 1,
        }
        supabase.table("feed_cards").update(update_payload).eq("id", row["id"]).execute()
        return False, True
    insert_payload = {**fields, "last_updated_at": now_iso}
    supabase.table("feed_cards").insert(insert_payload).execute()
    return True, False


def log_ingest_run(
    supabase, source: str, started: datetime, examined: int, created: int, updated: int, errors: int, status: str
) -> None:
    try:
        supabase.table("feed_ingestion_log").insert(
            {
                "source": source[:200],
                "ingest_started_at": started.isoformat(),
                "ingest_completed_at": datetime.now(timezone.utc).isoformat(),
                "records_examined": examined,
                "cards_created": created,
                "cards_updated": updated,
                "errors_count": errors,
                "status": status[:50],
            }
        ).execute()
    except Exception as e:
        LOG.warning("feed_ingestion_log insert failed: %s", e)


# --- Phase 1: transform existing legislative_activity -----------------------


def _county_for_official(supabase, name: str) -> str | None:
    """Best-effort county lookup for a given official name (cached)."""
    cache = _county_for_official.__dict__.setdefault("_cache", {})
    if name in cache:
        return cache[name]
    r = (
        supabase.table("elected_officials")
        .select("zip_codes")
        .eq("name", name)
        .eq("state", "FL")
        .limit(1)
        .execute()
    )
    if not r.data:
        cache[name] = None
        return None
    zips = (r.data[0].get("zip_codes") or "").split(",")
    if not zips or not zips[0]:
        cache[name] = None
        return None
    # Reuse county_zips lookup
    cz = _load_county_zips(supabase)
    tally: Counter = Counter()
    for z in zips:
        z = z.strip()
        for county, czips in cz.items():
            if z in czips:
                tally[county] += 1
    county = tally.most_common(1)[0][0] if tally else None
    cache[name] = county
    return county


_county_zips_cache: dict[str, set[str]] | None = None


def _load_county_zips(supabase) -> dict[str, set[str]]:
    global _county_zips_cache
    if _county_zips_cache is not None:
        return _county_zips_cache
    r = supabase.table("county_zips").select("county, zip_codes").execute()
    out: dict[str, set[str]] = {}
    for row in r.data or []:
        zips = set((row.get("zip_codes") or "").split(","))
        out[row["county"]] = {z.strip() for z in zips if z.strip()}
    _county_zips_cache = out
    return out


_city_zips_cache: dict[str, dict] | None = None


def _load_city_zips(supabase) -> dict[str, dict]:
    """zip -> {city, county}. Returns first match for each ZIP."""
    global _city_zips_cache
    if _city_zips_cache is not None:
        return _city_zips_cache
    r = supabase.table("city_zips").select("city, county, zip_codes").execute()
    out: dict[str, dict] = {}
    for row in r.data or []:
        for z in (row.get("zip_codes") or "").split(","):
            z = z.strip()
            if z and z not in out:
                out[z] = {"city": row["city"], "county": row["county"]}
    _city_zips_cache = out
    return out


def _bill_priority(status: str | None) -> int:
    s = (status or "").lower()
    if "enacted" in s or "signed" in s or "became law" in s or "public law" in s:
        return 1
    if "passed" in s and "chamber" in s:
        return 1
    if "passed" in s:
        return 2
    if "committee_reported" in s or "reported" in s:
        return 3
    if "in_committee" in s:
        return 4
    return 5


def _vote_priority(activity: dict) -> int:
    """Rough heuristic for vote importance from activity row."""
    title = (activity.get("title") or "").lower()
    status = (activity.get("status") or "").lower()
    if "passage" in title or "final" in title or "passed_chamber" in status:
        return 2
    if "amendment" in title or "motion" in title:
        return 4
    return 5


def _icon_for(card_type: str) -> str:
    return {
        "they_voted": "📜",
        "did_you_know": "💡",
        "breaking": "🔔",
        "digest": "📦",
        "update": "🔄",
    }.get(card_type, "📰")


def map_legislative_to_card(activity: dict, county: str | None) -> dict | None:
    a = activity
    name = a["official_name"]
    level = a.get("official_level")
    activity_type = a["activity_type"]
    bill = a.get("bill_number")
    title = a.get("title") or ""
    status = a.get("status")
    src = a.get("source")
    src_url = a.get("source_url")
    event_date = a.get("date")

    if activity_type == "bill_sponsored":
        body = title
        if status:
            body += f" — Status: {status}"
        return {
            "card_type": "they_voted",
            "title": f"{name} sponsored {bill}" if bill else f"{name} sponsored a bill",
            "body": body[:500],
            "icon": _icon_for("they_voted"),
            "county": county,
            "official_name": name,
            "official_level": level,
            "source": src,
            "source_url": src_url,
            "priority": _bill_priority(status),
            "active": True,
            "event_date": event_date,
            "bill_number": bill,
            "group_key": f"bills_by:{name}",
            "dedup_key": f"bill_sponsored:{name}:{bill or title[:40]}",
        }
    if activity_type == "bill_cosponsored":
        body = f"Cosponsored {title}".strip()
        if status:
            body += f" — Status: {status}"
        return {
            "card_type": "they_voted",
            "title": f"{name} cosponsored {bill}" if bill else f"{name} cosponsored a bill",
            "body": body[:500],
            "icon": _icon_for("they_voted"),
            "county": county,
            "official_name": name,
            "official_level": level,
            "source": src,
            "source_url": src_url,
            "priority": min(_bill_priority(status) + 1, 5),
            "active": True,
            "event_date": event_date,
            "bill_number": bill,
            "group_key": f"cosponsors_by:{name}",
            "dedup_key": f"bill_cosponsored:{name}:{bill or title[:40]}",
        }
    if activity_type == "vote":
        vp = a.get("vote_position") or "?"
        outcome = status or "outcome unknown"
        return {
            "card_type": "they_voted",
            "title": f"{name} voted {vp} on {bill}" if bill else f"{name} cast a vote",
            "body": f"{title}. Bill outcome: {outcome}.",
            "icon": _icon_for("they_voted"),
            "county": county,
            "official_name": name,
            "official_level": level,
            "source": src,
            "source_url": src_url,
            "priority": _vote_priority(a),
            "active": True,
            "event_date": event_date,
            "bill_number": bill,
            "group_key": f"votes_by:{name}",
            "dedup_key": f"vote:{name}:{bill or title[:40]}",
        }
    if activity_type == "committee":
        return {
            "card_type": "did_you_know",
            "title": f"{name} serves on {title}",
            "body": (a.get("description") or title)[:500],
            "icon": _icon_for("did_you_know"),
            "county": county,
            "official_name": name,
            "official_level": level,
            "source": src,
            "source_url": src_url,
            "priority": 6,
            "active": True,
            "event_date": event_date,
            "group_key": f"committee:{name}",
            "dedup_key": f"committee:{name}:{title[:60]}",
        }
    return None


def phase1_transform(supabase, dry_run: bool) -> dict:
    LOG.info("Phase 1: transform legislative_activity -> feed_cards")
    started = datetime.now(timezone.utc)
    examined = created = updated = errors = 0
    seen_dedups: set[str] = set()

    rows: list[dict] = []
    start = 0
    while True:
        r = supabase.table("legislative_activity").select("*").range(start, start + 999).execute()
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000

    LOG.info("  %d activity rows to process", len(rows))
    for i, act in enumerate(rows, 1):
        examined += 1
        try:
            county = _county_for_official(supabase, act["official_name"])
            card = map_legislative_to_card(act, county)
            if not card:
                continue
            if not is_allowed_source(card.get("source_url")):
                continue
            if card["dedup_key"] in seen_dedups:
                continue
            seen_dedups.add(card["dedup_key"])
            ins, upd = upsert_card(supabase, dry_run, **card)
            if ins:
                created += 1
            if upd:
                updated += 1
        except Exception as e:
            errors += 1
            LOG.warning("activity %s err: %s", act.get("id"), e)
        if i % 100 == 0:
            LOG.info("  [%d/%d] created=%d updated=%d errors=%d", i, len(rows), created, updated, errors)

    log_ingest_run(supabase, "phase1_legislative_activity", started, examined, created, updated, errors, "ok")
    LOG.info("Phase 1 done: created=%d updated=%d errors=%d", created, updated, errors)
    return {"created": created, "updated": updated, "errors": errors}


# --- Phase 2: smart grouping ------------------------------------------------


def phase2_grouping(supabase, dry_run: bool) -> dict:
    LOG.info("Phase 2: smart grouping (4+ same group_key in 14 days)")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("feed_cards")
            .select("id, group_key, official_name, title, bill_number, last_updated_at, created_at, priority")
            .eq("active", True)
            .neq("card_type", "digest")
            .gte("last_updated_at", cutoff)
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000

    by_group: dict[str, list[dict]] = defaultdict(list)
    for c in rows:
        if c.get("group_key"):
            by_group[c["group_key"]].append(c)

    digests_created = 0
    children_collapsed = 0
    for gkey, members in by_group.items():
        if len(members) < 4:
            continue
        members.sort(key=lambda x: (x.get("priority") or 5, x.get("title") or ""))
        top3 = members[:3]
        sample = ", ".join(
            (m.get("bill_number") or m.get("title") or "")[:40] for m in top3 if m
        )
        official = members[0].get("official_name") or "Official"
        # Determine activity verb from group_key prefix
        verb = "had activity"
        if gkey.startswith("bills_by:"):
            verb = f"sponsored {len(members)} bills recently"
        elif gkey.startswith("cosponsors_by:"):
            verb = f"cosponsored {len(members)} bills recently"
        elif gkey.startswith("votes_by:"):
            verb = f"cast {len(members)} votes recently"
        elif gkey.startswith("committee:"):
            continue  # don't digest committee assignments

        digest = {
            "card_type": "digest",
            "title": f"{official} {verb}",
            "body": f"Top: {sample}. View all {len(members)}.",
            "icon": _icon_for("digest"),
            "county": None,
            "official_name": official,
            "official_level": None,
            "source": "PolitiScore feed-engine digest",
            "source_url": None,
            "priority": 2,
            "active": True,
            "event_date": None,
            "group_key": gkey,
            "dedup_key": f"digest:{gkey}",
        }
        try:
            ins, upd = upsert_card(supabase, dry_run, **digest)
            if ins or upd:
                digests_created += 1
            if not dry_run:
                child_ids = [m["id"] for m in members]
                # Lower visibility for children — keep them but mark priority worse
                supabase.table("feed_cards").update({"priority": 8}).in_("id", child_ids).execute()
                children_collapsed += len(child_ids)
        except Exception as e:
            LOG.warning("digest upsert failed for %s: %s", gkey, e)

    LOG.info("Phase 2 done: digests=%d, children_collapsed=%d", digests_created, children_collapsed)
    return {"digests": digests_created, "collapsed": children_collapsed}


# --- Phase 3: accountability snapshot --------------------------------------


def phase3_snapshot(supabase, dry_run: bool) -> dict:
    LOG.info("Phase 3: snapshot accountability_metrics")
    today = date.today()
    started = datetime.now(timezone.utc)

    # Pull current metrics
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("accountability_metrics")
            .select(
                "official_id, official_name, metric_key, metric_label, metric_value, metric_unit, performance_rating, year, source"
            )
            .neq("performance_rating", "no_data")
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000

    LOG.info("  %d real metrics to snapshot", len(rows))

    cards_created = 0
    inserted = 0
    if not dry_run:
        # Bulk insert with ON CONFLICT DO NOTHING for idempotency within day
        chunks = [rows[i : i + 500] for i in range(0, len(rows), 500)]
        for chunk in chunks:
            payload = [
                {
                    "snapshot_date": today.isoformat(),
                    "official_id": r["official_id"],
                    "official_name": r.get("official_name"),
                    "metric_key": r["metric_key"],
                    "metric_label": r.get("metric_label"),
                    "metric_value": str(r.get("metric_value")) if r.get("metric_value") is not None else None,
                    "metric_unit": r.get("metric_unit"),
                    "performance_rating": r.get("performance_rating"),
                    "year": r.get("year"),
                    "source": r.get("source"),
                }
                for r in chunk
            ]
            try:
                supabase.table("accountability_metrics_snapshots").upsert(
                    payload, on_conflict="snapshot_date,official_id,metric_key,year"
                ).execute()
                inserted += len(payload)
            except Exception as e:
                LOG.warning("snapshot upsert failed: %s", e)

    # Compare against last week's snapshot
    week_ago = (today - timedelta(days=7)).isoformat()
    prev_rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("accountability_metrics_snapshots")
            .select("official_id, metric_key, metric_value, year, official_name, metric_label")
            .eq("snapshot_date", week_ago)
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        prev_rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000

    if not prev_rows:
        LOG.info("  no prior snapshot for %s — skipping change cards (first run)", week_ago)
        log_ingest_run(supabase, "phase3_snapshot", started, len(rows), 0, 0, 0, "ok_first_run")
        return {"snapshots_inserted": inserted, "change_cards": 0}

    prev_index = {(r["official_id"], r["metric_key"], r.get("year")): r["metric_value"] for r in prev_rows}
    for r in rows:
        key = (r["official_id"], r["metric_key"], r.get("year"))
        prev_val = prev_index.get(key)
        cur_val = str(r.get("metric_value")) if r.get("metric_value") is not None else None
        if prev_val is None or cur_val is None or prev_val == cur_val:
            continue
        try:
            ins, upd = upsert_card(
                supabase,
                dry_run,
                card_type="breaking",
                title=f"{r['official_name']}'s {r['metric_label']} updated",
                body=f"From {prev_val} to {cur_val} ({r.get('metric_unit') or ''}).",
                icon=_icon_for("breaking"),
                county=_county_for_official(supabase, r["official_name"]),
                official_name=r["official_name"],
                source="PolitiScore week-over-week scorecard",
                priority=1,
                active=True,
                event_date=today.isoformat(),
                related_metric_key=r["metric_key"],
                dedup_key=f"score_change:{r['official_id']}:{r['metric_key']}:{today.isoformat()}",
            )
            if ins:
                cards_created += 1
        except Exception as e:
            LOG.warning("score-change card failed: %s", e)

    log_ingest_run(supabase, "phase3_snapshot", started, len(rows), cards_created, 0, 0, "ok")
    LOG.info("Phase 3 done: snapshots=%d, change_cards=%d", inserted, cards_created)
    return {"snapshots_inserted": inserted, "change_cards": cards_created}


# --- Phase 4: live ingestion ------------------------------------------------


def http_get_json(url: str, params: dict | None = None, timeout: int = 25, retries: int = 2) -> dict | None:
    for attempt in range(retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                LOG.warning("429 from %s, sleeping %ds", url[:80], wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            if attempt == retries:
                LOG.warning("http_get_json failed %s: %s", url[:80], e)
            time.sleep(1)
    return None


def http_get_text(url: str, timeout: int = 20) -> str | None:
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.text
    except requests.RequestException as e:
        LOG.debug("http_get_text failed %s: %s", url[:80], e)
    return None


def phase4_congress(supabase, dry_run: bool, since_iso: str) -> dict:
    LOG.info("Phase 4a: Congress.gov sponsored-legislation since %s", since_iso)
    started = datetime.now(timezone.utc)
    if not CONGRESS_KEY:
        LOG.warning("CONGRESS_API_KEY missing, skipping")
        return {"created": 0, "updated": 0}

    members = supabase.table("elected_officials").select("name, level, zip_codes").eq("state", "FL").eq("level", "federal").execute()
    fl_federal = [
        m for m in (members.data or [])
        if "U.S. Senator" in (m.get("name", "") + " ") or "U.S. Representative" in (m.get("name", "") + " ")
        or True  # we'll filter via title below; simpler is keep all federal-level
    ]

    # Get FL members from Congress.gov for bioguide mapping
    bd = http_get_json(
        f"https://api.congress.gov/v3/member/congress/{CURRENT_CONGRESS}/FL",
        params={"api_key": CONGRESS_KEY, "format": "json", "limit": 100},
    )
    fl_members = (bd or {}).get("members", []) if bd else []

    def name_to_bioguide(target: str) -> str | None:
        target_tokens = re.findall(r"[a-z]+", target.lower())
        if not target_tokens:
            return None
        target_last = target_tokens[-1]
        for m in fl_members:
            raw = m.get("name", "")
            parts = [p.strip() for p in raw.split(",", 1)]
            cand_last = parts[0].lower() if parts else ""
            if cand_last == target_last:
                return m.get("bioguideId")
        return None

    created = updated = 0
    examined = 0
    # Process officials with bioguide match
    for m in members.data or []:
        bg = name_to_bioguide(m["name"])
        if not bg:
            continue
        d = http_get_json(
            f"https://api.congress.gov/v3/member/{bg}/sponsored-legislation",
            params={"api_key": CONGRESS_KEY, "format": "json", "limit": 25, "fromDateTime": since_iso},
        )
        time.sleep(1.0)
        if not d:
            continue
        for bill in d.get("sponsoredLegislation", []):
            examined += 1
            if bill.get("congress") != CURRENT_CONGRESS:
                continue
            bill_num = f"{(bill.get('type') or '').upper()} {bill.get('number') or ''}".strip()
            la = bill.get("latestAction") or {}
            status = la.get("text", "")
            event_date = la.get("actionDate") or bill.get("introducedDate")
            url = bill.get("url") or ""
            if "api.congress.gov" in url:
                url = url.replace("api.congress.gov/v3", "www.congress.gov").split("?")[0]
            try:
                county = _county_for_official(supabase, m["name"])
                ins, upd = upsert_card(
                    supabase,
                    dry_run,
                    card_type="they_voted",
                    title=f"{m['name']} sponsored {bill_num}",
                    body=(bill.get("title") or "") + (f" — Status: {status}" if status else ""),
                    icon=_icon_for("they_voted"),
                    county=county,
                    official_name=m["name"],
                    official_level="federal",
                    source="Congress.gov",
                    source_url=url if is_allowed_source(url) else None,
                    priority=_bill_priority(status),
                    active=True,
                    event_date=event_date,
                    bill_number=bill_num,
                    group_key=f"bills_by:{m['name']}",
                    dedup_key=f"bill_sponsored:{m['name']}:{bill_num}",
                )
                if ins:
                    created += 1
                if upd:
                    updated += 1
            except Exception as e:
                LOG.warning("congress upsert err for %s: %s", m["name"], e)

    log_ingest_run(supabase, "phase4_congress", started, examined, created, updated, 0, "ok")
    LOG.info("Phase 4a done: examined=%d created=%d updated=%d", examined, created, updated)
    return {"created": created, "updated": updated}


def phase4_openstates(supabase, dry_run: bool, since_date: str, batch_size: int = 50) -> dict:
    """Daily rotation: pick batch_size legislators (by id mod weekday) so all
    get covered within ~3-4 days while staying under 500/day quota."""
    LOG.info("Phase 4b: OpenStates (rotated batch=%d) since %s", batch_size, since_date)
    started = datetime.now(timezone.utc)
    if not OPENSTATES_KEY:
        LOG.warning("OPENSTATES_API_KEY missing, skipping")
        return {"created": 0, "updated": 0}

    # FL state legislators
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, zip_codes")
            .eq("state", "FL")
            .eq("level", "state")
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000
    leg = [
        o for o in rows
        if re.match(r"^(State Senator|Senator|State Representative|Representative)", o.get("title") or "")
    ]
    LOG.info("  %d FL state legislators in pool", len(leg))

    # Rotate by today's day-of-year
    today_idx = date.today().toordinal() % max(1, (len(leg) // batch_size + 1))
    batch = sorted(leg, key=lambda o: o["id"])[today_idx * batch_size : (today_idx + 1) * batch_size]
    LOG.info("  rotation slot %d -> processing %d officials", today_idx, len(batch))

    examined = created = updated = 0
    for off in batch:
        # Lookup person id (best-effort name match)
        d = http_get_json(
            "https://v3.openstates.org/people",
            params={"jurisdiction": "fl", "name": off["name"], "apikey": OPENSTATES_KEY},
        )
        time.sleep(1.5)
        if not d:
            continue
        results = d.get("results", [])
        if not results:
            continue
        person_id = results[0]["id"]
        # Fetch bills updated since
        d2 = http_get_json(
            "https://v3.openstates.org/bills",
            params={
                "jurisdiction": "fl",
                "sponsor": person_id,
                "updated_since": since_date,
                "per_page": 20,
                "apikey": OPENSTATES_KEY,
            },
        )
        time.sleep(1.5)
        if not d2:
            continue
        for bill in d2.get("results", []) or []:
            examined += 1
            ident = bill.get("identifier")
            title = bill.get("title") or ""
            la = bill.get("latest_action_description") or ""
            la_date = bill.get("latest_action_date")
            url = (bill.get("openstates_url") or
                   f"https://openstates.org/fl/bills/{bill.get('session','')}/{ident}/")
            try:
                county = _county_for_official(supabase, off["name"])
                ins, upd = upsert_card(
                    supabase,
                    dry_run,
                    card_type="they_voted",
                    title=f"{off['name']} sponsored {ident}",
                    body=f"{title} — Latest: {la}".strip(" —"),
                    icon=_icon_for("they_voted"),
                    county=county,
                    official_name=off["name"],
                    official_level="state",
                    source="OpenStates API",
                    source_url=url if is_allowed_source(url) else None,
                    priority=_bill_priority(la),
                    active=True,
                    event_date=la_date,
                    bill_number=ident,
                    group_key=f"bills_by:{off['name']}",
                    dedup_key=f"bill_sponsored:{off['name']}:{ident}",
                )
                if ins:
                    created += 1
                if upd:
                    updated += 1
            except Exception as e:
                LOG.warning("openstates upsert err: %s", e)

    log_ingest_run(supabase, "phase4_openstates", started, examined, created, updated, 0, "ok")
    LOG.info("Phase 4b done: examined=%d created=%d updated=%d", examined, created, updated)
    return {"created": created, "updated": updated}


def _parse_rss(text: str) -> list[dict]:
    """Minimal RSS/Atom parser. Returns list of {title, link, description, pubDate}."""
    items: list[dict] = []
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return items
    # RSS
    for item in root.iter("item"):
        items.append(
            {
                "title": (item.findtext("title") or "").strip(),
                "link": (item.findtext("link") or "").strip(),
                "description": (item.findtext("description") or "").strip(),
                "pubDate": (item.findtext("pubDate") or "").strip(),
            }
        )
    # Atom
    ns = "{http://www.w3.org/2005/Atom}"
    for entry in root.iter(ns + "entry"):
        link_el = entry.find(ns + "link")
        items.append(
            {
                "title": (entry.findtext(ns + "title") or "").strip(),
                "link": link_el.get("href") if link_el is not None else "",
                "description": (entry.findtext(ns + "summary") or "").strip(),
                "pubDate": (entry.findtext(ns + "updated") or entry.findtext(ns + "published") or "").strip(),
            }
        )
    return items


GOV_RSS_FEEDS = [
    ("FL Governor", "https://www.flgov.com/feed/", "Ron DeSantis", "state"),
    # Add per-official .gov RSS here as discovered
]


def phase4_rss(supabase, dry_run: bool, since_iso: str) -> dict:
    LOG.info("Phase 4c: .gov RSS feeds")
    started = datetime.now(timezone.utc)
    examined = created = updated = 0
    for label, url, owner, level in GOV_RSS_FEEDS:
        text = http_get_text(url)
        time.sleep(1.5)
        if not text:
            LOG.warning("RSS fetch failed: %s", url)
            continue
        items = _parse_rss(text)
        for it in items[:20]:
            examined += 1
            link = it["link"]
            if not is_allowed_source(link):
                continue
            title = it["title"][:200]
            if not title:
                continue
            try:
                ins, upd = upsert_card(
                    supabase,
                    dry_run,
                    card_type="breaking",
                    title=f"{owner}: {title}"[:300],
                    body=re.sub(r"<[^>]+>", "", it["description"])[:500],
                    icon=_icon_for("breaking"),
                    county=None,
                    official_name=owner,
                    official_level=level,
                    source=label,
                    source_url=link,
                    priority=2,
                    active=True,
                    event_date=None,
                    dedup_key=f"rss:{label}:{link}",
                )
                if ins:
                    created += 1
                if upd:
                    updated += 1
            except Exception as e:
                LOG.warning("rss upsert err: %s", e)
    log_ingest_run(supabase, "phase4_rss", started, examined, created, updated, 0, "ok")
    LOG.info("Phase 4c done: examined=%d created=%d updated=%d", examined, created, updated)
    return {"created": created, "updated": updated}


def phase4_all(supabase, dry_run: bool) -> dict:
    yesterday = date.today() - timedelta(days=1)
    since_iso = yesterday.isoformat() + "T00:00:00Z"
    since_date = yesterday.isoformat()
    a = phase4_congress(supabase, dry_run, since_iso)
    b = phase4_openstates(supabase, dry_run, since_date)
    c = phase4_rss(supabase, dry_run, since_iso)
    return {
        "created": a["created"] + b["created"] + c["created"],
        "updated": a["updated"] + b["updated"] + c["updated"],
    }


# --- Phase 5: daily brief --------------------------------------------------


def phase5_brief(supabase, dry_run: bool, only_zip: str | None = None) -> dict:
    LOG.info("Phase 5: daily brief per ZIP")
    started = datetime.now(timezone.utc)
    today = date.today()

    # Build ZIP universe
    cz = _load_county_zips(supabase)
    city_zip_map = _load_city_zips(supabase)
    all_zips: set[str] = set()
    for zips in cz.values():
        all_zips.update(zips)
    all_zips.update(city_zip_map.keys())
    if only_zip:
        all_zips = {only_zip} if only_zip in all_zips else {only_zip}

    LOG.info("  %d ZIPs to brief", len(all_zips))

    # Pull last-7-day cards once
    seven_ago = (today - timedelta(days=7)).isoformat()
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("feed_cards")
            .select(
                "id, card_type, title, body, official_name, county, priority, last_updated_at, event_date, related_metric_key"
            )
            .eq("active", True)
            .gte("last_updated_at", seven_ago)
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000
    LOG.info("  %d candidate cards from last 7 days", len(rows))

    # Build per-official ZIP map
    off_rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("name, zip_codes")
            .eq("state", "FL")
            .range(start, start + 999)
            .execute()
        )
        if not r.data:
            break
        off_rows.extend(r.data)
        if len(r.data) < 1000:
            break
        start += 1000

    zip_to_officials: dict[str, set[str]] = defaultdict(set)
    for o in off_rows:
        for z in (o.get("zip_codes") or "").split(","):
            z = z.strip()
            if z:
                zip_to_officials[z].add(o["name"])

    cards_by_official: dict[str, list[dict]] = defaultdict(list)
    cards_by_county: dict[str, list[dict]] = defaultdict(list)
    for c in rows:
        if c.get("official_name"):
            cards_by_official[c["official_name"]].append(c)
        if c.get("county"):
            cards_by_county[c["county"]].append(c)

    def score_card(c: dict) -> float:
        prio = c.get("priority") or 5
        last = c.get("last_updated_at") or ""
        recency = 0
        try:
            ts = datetime.fromisoformat(last.replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - ts).days
            recency = max(0, 7 - age_days) * 0.5
        except (ValueError, TypeError):
            pass
        return -(prio - recency)  # higher is better

    briefs_inserted = 0
    used_card_ids_by_zip: dict[str, set[int]] = defaultdict(set)
    # Avoid same brief twice in same week per ZIP
    week_briefs = (
        supabase.table("daily_brief_history")
        .select("zip_code, feed_card_id")
        .gte("brief_date", (today - timedelta(days=6)).isoformat())
        .execute()
    )
    for w in week_briefs.data or []:
        used_card_ids_by_zip[w["zip_code"]].add(w["feed_card_id"])

    for zip_code in sorted(all_zips):
        # Determine candidate cards
        info = city_zip_map.get(zip_code)
        county = info["county"] if info else None
        candidates: list[dict] = []
        for off_name in zip_to_officials.get(zip_code, set()):
            candidates.extend(cards_by_official.get(off_name, []))
        if county:
            candidates.extend(cards_by_county.get(county, []))
        # Deduplicate by id
        seen: set[int] = set()
        uniq = []
        for c in candidates:
            if c["id"] in seen:
                continue
            seen.add(c["id"])
            uniq.append(c)
        # Skip already-used cards this week
        avail = [c for c in uniq if c["id"] not in used_card_ids_by_zip.get(zip_code, set())]
        # Skip cards we'd be using twice in same minute for same ZIP
        avail.sort(key=score_card, reverse=True)
        chosen = avail[0] if avail else None

        # Evergreen fallback
        brief_type = "card"
        if not chosen:
            # Pick a real accountability metric for an official in this ZIP
            for off_name in zip_to_officials.get(zip_code, set()):
                m = (
                    supabase.table("accountability_metrics")
                    .select("metric_label, metric_value, metric_unit, year, performance_rating, official_id")
                    .eq("official_name", off_name)
                    .neq("performance_rating", "no_data")
                    .order("year", desc=True)
                    .limit(1)
                    .execute()
                )
                if m.data:
                    metric = m.data[0]
                    label = metric.get("metric_label") or metric.get("metric_key") or "metric"
                    val = metric.get("metric_value") or "?"
                    unit = metric.get("metric_unit") or ""
                    # Insert evergreen feed_card and use that
                    eg = {
                        "card_type": "did_you_know",
                        "title": f"This week: {off_name}'s {label} = {val}{unit}",
                        "body": f"Pulled from {off_name}'s scorecard for ZIP {zip_code}.",
                        "icon": _icon_for("did_you_know"),
                        "county": county,
                        "official_name": off_name,
                        "source": "PolitiScore evergreen",
                        "priority": 6,
                        "active": True,
                        "event_date": today.isoformat(),
                        "dedup_key": f"evergreen:{off_name}:{label}:{today.isoformat()}",
                    }
                    if not dry_run:
                        try:
                            upsert_card(supabase, dry_run, **eg)
                            r = (
                                supabase.table("feed_cards")
                                .select("id, card_type, title, body, priority, last_updated_at, event_date")
                                .eq("dedup_key", eg["dedup_key"])
                                .limit(1)
                                .execute()
                            )
                            if r.data:
                                chosen = r.data[0]
                                brief_type = "evergreen"
                        except Exception as e:
                            LOG.warning("evergreen upsert err: %s", e)
                    break

        if not chosen:
            continue

        # Insert brief history
        if not dry_run:
            try:
                supabase.table("daily_brief_history").upsert(
                    {
                        "brief_date": today.isoformat(),
                        "zip_code": zip_code,
                        "feed_card_id": chosen["id"],
                        "brief_type": brief_type,
                    },
                    on_conflict="brief_date,zip_code",
                ).execute()
                briefs_inserted += 1
            except Exception as e:
                LOG.warning("brief insert failed for %s: %s", zip_code, e)

    log_ingest_run(supabase, "phase5_brief", started, len(all_zips), briefs_inserted, 0, 0, "ok")
    LOG.info("Phase 5 done: briefs_inserted=%d", briefs_inserted)
    return {"briefs": briefs_inserted}


# --- Phase 6: coming up ----------------------------------------------------


def phase6_coming_up(supabase, dry_run: bool) -> dict:
    LOG.info("Phase 6: coming up events")
    started = datetime.now(timezone.utc)
    inserted = 0
    examined = 0

    # FL Senate calendar (best-effort scrape; the page is HTML-only)
    text = http_get_text("https://www.flsenate.gov/Session/Calendar")
    if text:
        # Look for date/event lines
        for m in re.finditer(
            r"<a[^>]*>(\d{1,2}/\d{1,2}/\d{4})</a>[\s\S]{0,500}?<td[^>]*>([^<]+)</td>",
            text,
        ):
            examined += 1
            try:
                d = datetime.strptime(m.group(1), "%m/%d/%Y").date()
                if d < date.today() or d > date.today() + timedelta(days=14):
                    continue
                desc = re.sub(r"\s+", " ", m.group(2)).strip()[:300]
                if not dry_run:
                    supabase.table("coming_up_events").upsert(
                        {
                            "title": f"FL Senate: {desc[:100]}",
                            "description": desc,
                            "event_type": "fl_senate_calendar",
                            "event_date": d.isoformat(),
                            "jurisdiction": "FL",
                            "source": "FL Senate Calendar",
                            "source_url": "https://www.flsenate.gov/Session/Calendar",
                            "active": True,
                        },
                        on_conflict="event_date,event_type,related_bill_number,related_official_name",
                    ).execute()
                    inserted += 1
            except (ValueError, Exception) as e:
                LOG.debug("flsenate parse err: %s", e)

    # FL House schedule
    text = http_get_text("https://www.myfloridahouse.gov/Sections/HouseSchedule/houseschedule.aspx")
    if text:
        for m in re.finditer(
            r"(\d{1,2}/\d{1,2}/\d{4})[\s\S]{0,300}?<td[^>]*>([^<]{10,200})</td>",
            text,
        ):
            examined += 1
            try:
                d = datetime.strptime(m.group(1), "%m/%d/%Y").date()
                if d < date.today() or d > date.today() + timedelta(days=14):
                    continue
                desc = re.sub(r"\s+", " ", m.group(2)).strip()[:300]
                if not dry_run:
                    supabase.table("coming_up_events").upsert(
                        {
                            "title": f"FL House: {desc[:100]}",
                            "description": desc,
                            "event_type": "fl_house_schedule",
                            "event_date": d.isoformat(),
                            "jurisdiction": "FL",
                            "source": "FL House Schedule",
                            "source_url": "https://www.myfloridahouse.gov/Sections/HouseSchedule/houseschedule.aspx",
                            "active": True,
                        },
                        on_conflict="event_date,event_type,related_bill_number,related_official_name",
                    ).execute()
                    inserted += 1
            except (ValueError, Exception) as e:
                LOG.debug("flhouse parse err: %s", e)

    # US House daily floor (docs.house.gov is HTML)
    text = http_get_text("https://docs.house.gov/floor/")
    if text:
        for m in re.finditer(r"(\d{1,2}/\d{1,2}/\d{4})", text):
            try:
                d = datetime.strptime(m.group(1), "%m/%d/%Y").date()
                if d < date.today() or d > date.today() + timedelta(days=14):
                    continue
                examined += 1
                if not dry_run:
                    supabase.table("coming_up_events").upsert(
                        {
                            "title": "U.S. House floor activity",
                            "description": "See docs.house.gov/floor",
                            "event_type": "us_house_floor",
                            "event_date": d.isoformat(),
                            "jurisdiction": "US",
                            "source": "docs.house.gov/floor",
                            "source_url": "https://docs.house.gov/floor/",
                            "active": True,
                        },
                        on_conflict="event_date,event_type,related_bill_number,related_official_name",
                    ).execute()
                    inserted += 1
                    break  # one per page is enough
            except ValueError:
                pass

    log_ingest_run(supabase, "phase6_coming_up", started, examined, inserted, 0, 0, "ok")
    LOG.info("Phase 6 done: events_inserted=%d", inserted)
    return {"events": inserted}


# --- Phase 7: cleanup ------------------------------------------------------


def phase7_cleanup(supabase, dry_run: bool) -> dict:
    LOG.info("Phase 7: expiry / cleanup")
    started = datetime.now(timezone.utc)
    today = date.today()

    deactivated = 0
    deleted_briefs = 0
    deleted_inactive = 0

    # Mark cards inactive where event_date < 30 days ago AND priority > 2
    cutoff_30 = (today - timedelta(days=30)).isoformat()
    cutoff_180 = (today - timedelta(days=180)).isoformat()
    cutoff_brief = (today - timedelta(days=365)).isoformat()

    if not dry_run:
        try:
            r = (
                supabase.table("feed_cards")
                .update({"active": False})
                .eq("active", True)
                .lt("event_date", cutoff_30)
                .gt("priority", 2)
                .execute()
            )
            deactivated = len(r.data) if r.data else 0
        except Exception as e:
            LOG.warning("deactivate failed: %s", e)

        try:
            r = (
                supabase.table("daily_brief_history")
                .delete()
                .lt("brief_date", cutoff_brief)
                .execute()
            )
            deleted_briefs = len(r.data) if r.data else 0
        except Exception as e:
            LOG.warning("delete briefs failed: %s", e)

        try:
            r = (
                supabase.table("feed_cards")
                .delete()
                .eq("active", False)
                .lt("last_updated_at", cutoff_180)
                .execute()
            )
            deleted_inactive = len(r.data) if r.data else 0
        except Exception as e:
            LOG.warning("delete inactive failed: %s", e)

    log_ingest_run(
        supabase, "phase7_cleanup", started, 0, 0, deactivated, 0, "ok"
    )
    LOG.info(
        "Phase 7 done: deactivated=%d deleted_briefs=%d deleted_inactive=%d",
        deactivated, deleted_briefs, deleted_inactive,
    )
    return {"deactivated": deactivated, "deleted_briefs": deleted_briefs, "deleted_inactive": deleted_inactive}


# --- Driver ----------------------------------------------------------------


PHASES = {
    "1": phase1_transform,
    "2": phase2_grouping,
    "3": phase3_snapshot,
    "4": phase4_all,
    "5": phase5_brief,
    "6": phase6_coming_up,
    "7": phase7_cleanup,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", default="all", help="1|2|3|4|5|6|7|all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--zip", default=None, help="Only this ZIP for Phase 5")
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        LOG.error("Missing Supabase env vars")
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    overall_started = datetime.now(timezone.utc)

    summary: dict[str, dict] = {}
    if args.phase == "all":
        order = ["1", "2", "3", "4", "5", "6", "7"]
    else:
        order = [args.phase]

    for ph in order:
        fn = PHASES.get(ph)
        if not fn:
            LOG.error("unknown phase %s", ph)
            return 1
        try:
            if ph == "5":
                summary[ph] = fn(supabase, args.dry_run, only_zip=args.zip)
            else:
                summary[ph] = fn(supabase, args.dry_run)
        except Exception as e:
            LOG.exception("Phase %s failed: %s", ph, e)
            summary[ph] = {"error": str(e)}

    duration = (datetime.now(timezone.utc) - overall_started).total_seconds()
    LOG.info("=" * 60)
    LOG.info("Feed Engine run complete in %.1fs", duration)
    for ph, res in summary.items():
        LOG.info("  Phase %s: %s", ph, res)
    return 0


if __name__ == "__main__":
    sys.exit(main())
