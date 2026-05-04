"""Ingest FL state-legislator campaign finance from FollowTheMoney.

For each FL state Representative or Senator in elected_officials:
  1. Search FTM by last name with c-r-oc=FL filter
  2. Match by full name; pick the candidate record with the highest total_raised
     when multiple records returned for the same name
  3. Fetch entity details for the matched eid
  4. Upsert into campaign_finance with cycle=2024

Note (2026-05-03): the API key embedded in the spec returns
`totalRecords=0` for every candidate-search variant tried during pre-flight
probing. The script is correct per spec; if FTM's search endpoint becomes
fully available again, results will populate automatically.

Usage: python scripts/ingest_fl_state_finance.py
"""

from __future__ import annotations

import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
FTM_API_KEY = "92c6de20f2f9abf57cccc638d0f2b343"

FTM_BASE = "https://api.followthemoney.org"
REQ_DELAY = 0.5
HTTP_TIMEOUT = 30
CYCLE = 2024

UA = {"User-Agent": "PolitiCard-ingest/0.1", "Accept": "application/json"}


def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z\s]", " ", s.lower()).strip()


def _name_tokens(s: str) -> list[str]:
    return [t for t in _norm(s).split() if len(t) > 1]


def _fetch_target_officials(supabase) -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        r = (
            supabase.table("elected_officials")
            .select("id, name, title, level, state, district, party")
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
    out: list[dict] = []
    for o in rows:
        t = (o.get("title") or "")
        if "Representative" in t or "Senator" in t:
            out.append(o)
    return out


def ftm_search(last_name: str) -> list[dict]:
    """FTM candidate search by last name within FL races."""
    r = requests.get(
        f"{FTM_BASE}/",
        params={
            "s": last_name,
            "c-r-oc": "FL",
            "APIKey": FTM_API_KEY,
            "mode": "json",
        },
        headers=UA,
        timeout=HTTP_TIMEOUT,
    )
    if not r.ok:
        return []
    try:
        d = r.json()
    except Exception:
        return []
    recs = d.get("records") or []
    return [x for x in recs if isinstance(x, dict)]


def ftm_entity(eid: int | str) -> dict | None:
    r = requests.get(
        f"{FTM_BASE}/entity.php",
        params={"eid": eid, "APIKey": FTM_API_KEY, "mode": "json"},
        headers=UA,
        timeout=HTTP_TIMEOUT,
    )
    if not r.ok:
        return None
    try:
        return r.json()
    except Exception:
        return None


def _record_name(rec: dict) -> str:
    """Pull a candidate name from an FTM record. Schema varies across query
    types so try common fields."""
    for k in ("Candidate", "candidate", "name", "EntityName"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v
        if isinstance(v, dict):
            for kk in ("Candidate", "candidate", "name", "EntityName"):
                vv = v.get(kk)
                if isinstance(vv, str) and vv.strip():
                    return vv
    return ""


def _record_eid(rec: dict) -> str | None:
    for k in ("eid", "EID", "candidate_id", "id"):
        v = rec.get(k)
        if isinstance(v, (str, int)) and str(v).strip():
            return str(v)
        if isinstance(v, dict):
            for kk in ("id", "eid"):
                vv = v.get(kk)
                if isinstance(vv, (str, int)) and str(vv).strip():
                    return str(vv)
    return None


def _record_total_raised(rec: dict) -> float:
    for k in ("total_raised", "TotalRaised", "raised", "total"):
        v = rec.get(k)
        try:
            if v is not None and str(v).strip():
                return float(str(v).replace(",", "").replace("$", ""))
        except ValueError:
            pass
    return 0.0


def _entity_total_raised(entity: dict) -> float | None:
    """Best-effort total_raised extraction from entity.php response."""
    if not entity:
        return None
    data = entity.get("data") or {}
    for path in (
        ("overview", "totals", "total_raised"),
        ("overview", "TotalRaised"),
        ("totals", "total_raised"),
    ):
        cur: object = data
        for p in path:
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                cur = None
                break
        try:
            if cur is not None:
                return float(str(cur).replace(",", "").replace("$", ""))
        except (TypeError, ValueError):
            pass
    return None


def _entity_top_industries(entity: dict) -> list[dict]:
    if not entity:
        return []
    data = entity.get("data") or {}
    overview = data.get("overview") or {}
    industries = overview.get("industry") or data.get("industry") or []
    out: list[dict] = []
    if isinstance(industries, list):
        for it in industries[:10]:
            if isinstance(it, dict):
                out.append({
                    "name": it.get("CatCodeBusiness") or it.get("industry") or it.get("name"),
                    "industry_id": it.get("industry_id"),
                    "amount": it.get("amount") or it.get("total"),
                })
    return out


def match_candidate(official_name: str, recs: list[dict]) -> dict | None:
    """Pick the record best matching the official's full name; tie-break by
    highest total_raised."""
    if not recs:
        return None
    target = _name_tokens(official_name)
    if not target:
        return None
    target_first, target_last = target[0], target[-1]
    scored: list[tuple[float, dict]] = []
    for rec in recs:
        rname = _record_name(rec)
        rtoks = _name_tokens(rname)
        if not rtoks:
            continue
        if rtoks[-1] != target_last:
            continue
        first_match = rtoks[0] == target_first or rtoks[0].startswith(target_first[0])
        if not first_match:
            continue
        scored.append((_record_total_raised(rec), rec))
    if not scored:
        return None
    scored.sort(key=lambda t: t[0], reverse=True)
    return scored[0][1]


def upsert_finance(supabase, *, official_id: int, official_name: str,
                   eid: str, total_raised: float | None,
                   top_industries: list[dict]) -> None:
    row = {
        "official_id": official_id,
        "official_name": official_name,
        "cycle": CYCLE,
        "total_raised": total_raised,
        "top_industries": top_industries,
        "source": "followthemoney",
        "source_url": f"https://www.followthemoney.org/entity-details?eid={eid}",
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    supabase.table("campaign_finance").delete() \
        .eq("official_id", official_id).eq("cycle", CYCLE).execute()
    supabase.table("campaign_finance").insert(row).execute()


def main() -> int:
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("ERROR: SUPABASE_URL / SUPABASE_SERVICE_KEY missing", file=sys.stderr)
        return 1
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    targets = _fetch_target_officials(supabase)
    print(f"Targets: {len(targets)} FL state legislators")

    matched: list[tuple[int, str, str, float | None]] = []
    skipped: list[tuple[int, str, str]] = []  # (id, name, reason)

    for i, off in enumerate(targets, 1):
        name = off["name"]
        toks = _name_tokens(name)
        if not toks:
            skipped.append((off["id"], name, "no name tokens"))
            continue
        last = toks[-1].title()
        try:
            recs = ftm_search(last)
        except Exception as e:
            skipped.append((off["id"], name, f"search error: {e}"))
            print(f"  [{i}/{len(targets)}] {name}: search error {e}")
            continue
        time.sleep(REQ_DELAY)

        if not recs:
            skipped.append((off["id"], name, "no FTM hits"))
            print(f"  [{i}/{len(targets)}] {name}: no FTM hits")
            continue

        cand = match_candidate(name, recs)
        if not cand:
            skipped.append((off["id"], name, "no name match"))
            print(f"  [{i}/{len(targets)}] {name}: {len(recs)} hit(s), no name match")
            continue

        eid = _record_eid(cand)
        if not eid:
            skipped.append((off["id"], name, "match has no eid"))
            print(f"  [{i}/{len(targets)}] {name}: match has no eid")
            continue

        try:
            entity = ftm_entity(eid)
        except Exception as e:
            skipped.append((off["id"], name, f"entity error: {e}"))
            print(f"  [{i}/{len(targets)}] {name}: entity error {e}")
            continue
        time.sleep(REQ_DELAY)

        total_raised = _entity_total_raised(entity)
        if total_raised is None:
            total_raised = _record_total_raised(cand) or None
        industries = _entity_top_industries(entity)

        try:
            upsert_finance(
                supabase,
                official_id=off["id"],
                official_name=name,
                eid=eid,
                total_raised=total_raised,
                top_industries=industries,
            )
            matched.append((off["id"], name, eid, total_raised))
            print(f"  [{i}/{len(targets)}] {name}: eid={eid} raised=${(total_raised or 0):,.0f}")
        except Exception as e:
            skipped.append((off["id"], name, f"upsert error: {e}"))
            print(f"  [{i}/{len(targets)}] {name}: upsert error {e}")

    print("\n=== SUMMARY ===")
    print(f"  matched: {len(matched)}")
    print(f"  skipped: {len(skipped)}")
    if skipped:
        from collections import Counter
        reasons = Counter(s[2].split(":")[0] for s in skipped)
        print("  skip reasons:")
        for r, n in reasons.most_common():
            print(f"    {n:>4}  {r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
