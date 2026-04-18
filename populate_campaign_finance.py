"""Populate campaign_finance for FL federal officials (cycle 2024) from FEC API.

Usage: python populate_campaign_finance.py

Requires env: SUPABASE_URL, SUPABASE_SERVICE_KEY, FEC_API_KEY (defaults to DEMO_KEY).
"""

import os
import re
import sys
import time
import json
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

from app.services.fec_client import get_top_pacs

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
FEC_API_KEY = os.getenv("FEC_API_KEY", "DEMO_KEY")

CYCLE = 2024
FEC_BASE = "https://api.open.fec.gov/v1"

# 28 FL federal officials (level=federal, state=FL, excluding ids 2 & 3).
# Office: S = senate, H = house.
OFFICIALS = [
    (4, "Ashley Moody", "S"),
    (5, "Rick Scott", "S"),
    (6, "Jimmy Patronis", "H"),
    (7, "Neal Dunn", "H"),
    (8, "Kat Cammack", "H"),
    (9, "Aaron Bean", "H"),
    (10, "John Rutherford", "H"),
    (12, "Cory Mills", "H"),
    (13, "Mike Haridopolos", "H"),
    (14, "Darren Soto", "H"),
    (15, "Anna Paulina Luna", "H"),
    (16, "Kathy Castor", "H"),
    (17, "Scott Franklin", "H"),
    (18, "Vern Buchanan", "H"),
    (19, "Greg Steube", "H"),
    (20, "Brian Mast", "H"),
    (21, "Lois Frankel", "H"),
    (22, "Jared Moskowitz", "H"),
    (23, "Frederica Wilson", "H"),
    (24, "Mario Diaz-Balart", "H"),
    (26, "Maria Elvira Salazar", "H"),
    (27, "Randy Fine", "H"),
    (462, "Maxwell Frost", "H"),
    (463, "Daniel Webster", "H"),
    (464, "Gus Bilirakis", "H"),
    (466, "Byron Donalds", "H"),
    (467, "Sheila Cherfilus-McCormick", "H"),
    (468, "Carlos Gimenez", "H"),
]


def _tokens(s: str) -> set[str]:
    return set(re.findall(r"[a-z]+", (s or "").lower()))


def _fec_get(path: str, params: dict | None = None, max_retries: int = 6) -> dict:
    p = {"api_key": FEC_API_KEY}
    if params:
        p.update(params)
    url = f"{FEC_BASE}{path}"
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=p, timeout=60)
        except requests.RequestException as e:
            last_exc = e
            wait = 5 * (attempt + 1)
            print(f"    [network error {e}, retry in {wait}s]", flush=True)
            time.sleep(wait)
            continue
        if r.status_code == 429:
            wait = min(60 * (attempt + 1), 300)
            print(f"    [rate-limited, sleeping {wait}s]", flush=True)
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [server {r.status_code}, retry in {wait}s]", flush=True)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"FEC API exhausted retries for {path}: {last_exc}")


def search_candidate_id(name: str, office: str) -> tuple[str | None, str | None]:
    """Resolve an FEC candidate_id for a given official in a cycle."""
    q_terms = name.split()[-1]  # last name often enough
    data = _fec_get(
        "/candidates/search/",
        {
            "q": q_terms,
            "state": "FL",
            "cycle": CYCLE,
            "office": office,
            "per_page": 50,
        },
    )
    results = data.get("results", []) or []
    if not results:
        return None, None
    target = _tokens(name)
    # Best match: all name tokens present in candidate name
    best = None
    best_score = -1
    for r in results:
        ct = _tokens(r.get("name", ""))
        if not target.issubset(ct):
            continue
        # Prefer candidates with a principal committee, break ties by shortest name
        score = 100 - len(ct)
        if score > best_score:
            best_score = score
            best = r
    if best is None:
        # Fallback: token-overlap ranking
        for r in results:
            score = len(target & _tokens(r.get("name", "")))
            if score > best_score:
                best_score = score
                best = r
    if best is None:
        return None, None
    return best.get("candidate_id"), best.get("name")


def fetch_totals(candidate_id: str) -> dict | None:
    data = _fec_get(
        f"/candidate/{candidate_id}/totals/",
        {"cycle": CYCLE, "per_page": 5},
    )
    results = data.get("results", []) or []
    # Return the row whose cycle matches CYCLE if present; else first
    for r in results:
        if int(r.get("cycle") or 0) == CYCLE:
            return r
    return results[0] if results else None


def fetch_principal_committees(candidate_id: str) -> list[str]:
    data = _fec_get(
        f"/candidate/{candidate_id}/committees/",
        {"cycle": CYCLE, "designation": "P"},
    )
    return [r["committee_id"] for r in (data.get("results") or []) if r.get("committee_id")]


def fetch_top_donors(candidate_id: str, n: int = 10) -> list[dict]:
    """Fetch top 100 individual contribution transactions for the candidate's
    principal committee(s), aggregate by contributor_name, return top ``n``."""
    committees = fetch_principal_committees(candidate_id)
    if not committees:
        return []
    agg: dict[str, dict] = {}
    for cid in committees:
        data = _fec_get(
            "/schedules/schedule_a/",
            {
                "committee_id": cid,
                "two_year_transaction_period": CYCLE,
                "is_individual": "true",
                "sort": "-contribution_receipt_amount",
                "per_page": 100,
            },
        )
        for r in data.get("results", []) or []:
            name = (r.get("contributor_name") or "").strip()
            if not name:
                continue
            amount = float(r.get("contribution_receipt_amount") or 0)
            if amount <= 0:
                continue
            state = (r.get("contributor_state") or "").strip().upper()
            cur = agg.setdefault(name, {"amount": 0.0, "state": state})
            cur["amount"] += amount
            if not cur["state"] and state:
                cur["state"] = state
    top = sorted(agg.items(), key=lambda kv: -kv[1]["amount"])[:n]
    return [
        {"name": k, "amount": round(v["amount"], 2), "state": v["state"]}
        for k, v in top
    ]


def _num(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f else None
    except (TypeError, ValueError):
        return None


def upsert(supabase, row: dict) -> None:
    supabase.table("campaign_finance").delete() \
        .eq("official_id", row["official_id"]) \
        .eq("cycle", row["cycle"]) \
        .execute()
    supabase.table("campaign_finance").insert(row).execute()


def process_one(supabase, oid: int, name: str, office: str) -> str:
    cand_id, cand_name = search_candidate_id(name, office)
    if not cand_id:
        return "skipped: no FEC candidate match"
    print(f"  FEC: {cand_id}  ({cand_name})", flush=True)

    totals = fetch_totals(cand_id)
    if not totals:
        return f"skipped: no 2024 totals for {cand_id}"

    try:
        donors = fetch_top_donors(cand_id)
    except Exception as e:
        print(f"    [donor fetch failed: {e}]", flush=True)
        donors = []

    raised = _num(totals.get("receipts"))
    spent = _num(totals.get("disbursements"))
    coh = _num(totals.get("last_cash_on_hand_end_period"))
    ind = _num(totals.get("individual_contributions"))
    pac = _num(totals.get("other_political_committee_contributions"))
    self_funded = _num(totals.get("candidate_contribution"))

    try:
        pacs = get_top_pacs(cand_id, CYCLE, total_raised=raised)
    except Exception as e:
        print(f"    [pac fetch failed: {e}]", flush=True)
        pacs = []

    print(
        f"  raised=${(raised or 0):,.0f}  spent=${(spent or 0):,.0f}  "
        f"coh=${(coh or 0):,.0f}  donors={len(donors)}  pacs={len(pacs)}",
        flush=True,
    )

    row = {
        "official_id": oid,
        "official_name": name,
        "fec_candidate_id": cand_id,
        "cycle": CYCLE,
        "total_raised": raised,
        "total_spent": spent,
        "cash_on_hand": coh,
        "individual_contributions": ind,
        "pac_contributions": pac,
        "self_funded": self_funded,
        "top_donors": donors,
        "top_pacs": pacs,
        "source": "FEC",
        "source_url": f"https://www.fec.gov/data/candidate/{cand_id}/?cycle={CYCLE}",
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    upsert(supabase, row)
    return f"ok ({len(donors)} donors, {len(pacs)} pacs)"


def main() -> int:
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        return 1
    if FEC_API_KEY == "DEMO_KEY":
        print("WARNING: using FEC DEMO_KEY (30 req/hr). Set FEC_API_KEY in .env for speed.")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    ok = 0
    failed: list[tuple[int, str, str]] = []
    started = time.time()

    for i, (oid, name, office) in enumerate(OFFICIALS, 1):
        print(f"\n[{i}/{len(OFFICIALS)}] {name}  (id={oid}, office={office})", flush=True)
        try:
            result = process_one(supabase, oid, name, office)
            if result.startswith("ok"):
                ok += 1
                print(f"  -> {result}", flush=True)
            else:
                failed.append((oid, name, result))
                print(f"  -> {result}", flush=True)
        except Exception as e:
            failed.append((oid, name, f"exception: {e}"))
            print(f"  -> ERROR: {e}", flush=True)
        time.sleep(0.4)

    elapsed = time.time() - started
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.0f}s. Upserted: {ok}/{len(OFFICIALS)}")
    if failed:
        print("Failures:")
        for oid, name, reason in failed:
            print(f"  {oid:>4}  {name:<32}  {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
