"""Ingest FEC schedule_b disbursements -> campaign_finance.top_expenditures.

For each FL federal official with an fec_candidate_id in campaign_finance:
  1. Page through /schedules/schedule_b/ filtered to the official's cycle
  2. Aggregate top 10 payees by total amount
  3. Bucket spending into product-friendly categories
     (media/ads, payroll, consulting, travel, fundraising, other)
  4. Write {top_payees, by_category, total_transactions} to top_expenditures

The schedule_b endpoint requires a cycle filter — without
two_year_transaction_period the API returns historical records mixed across
filing periods. We use the cycle stored on the campaign_finance row.

Usage: python scripts/ingest_fec_disbursements.py
"""

from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
FEC_API_KEY = os.getenv("FEC_API_KEY") or "Nkk7v4dGV626lvPiw2uv0H7JvuYZEkKqQlcpE9oo"

FEC_BASE = "https://api.open.fec.gov/v1"
REQ_DELAY = 0.5
HTTP_TIMEOUT = 60
MAX_PAGES_PER_OFFICIAL = 50  # 100/page * 50 = 5,000 disbursements cap

UA = {"User-Agent": "PolitiCard-ingest/0.1", "Accept": "application/json"}


# Map of FEC purpose-category / description keywords to our 6 buckets.
# Order matters — first hit wins.
CATEGORY_RULES = (
    ("media", ("ADVERTISING", "MEDIA", "AD BUY", "TV", "RADIO", "DIGITAL ADS",
               "FACEBOOK", "GOOGLE", "PRINT")),
    ("payroll", ("PAYROLL", "SALARY", "SALARIES", "WAGES", "STAFF",
                 "EMPLOYEE", "PERSONNEL")),
    ("consulting", ("CONSULTING", "CONSULTANT", "STRATEGY", "POLLING",
                    "RESEARCH", "LEGAL", "ATTORNEY", "ACCOUNTING")),
    ("travel", ("TRAVEL", "AIRFARE", "LODGING", "HOTEL", "MILEAGE",
                "TRANSPORTATION", "MEALS")),
    ("fundraising", ("FUNDRAISING", "FUNDRAISER", "DONOR", "EVENT",
                     "CATERING", "SOLICITATION")),
)


def _bucket(disb: dict) -> str:
    parts = [
        (disb.get("disbursement_purpose_category") or ""),
        (disb.get("disbursement_description") or ""),
    ]
    text = " ".join(parts).upper()
    for bucket, needles in CATEGORY_RULES:
        for n in needles:
            if n in text:
                return bucket
    return "other"


def _amt(v) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _fetch_principal_committees(cand_id: str) -> list[str]:
    """Return the candidate's principal-committee IDs (designation=P)."""
    try:
        r = requests.get(
            f"{FEC_BASE}/candidate/{cand_id}/committees/",
            params={"api_key": FEC_API_KEY, "designation": "P"},
            timeout=HTTP_TIMEOUT, headers=UA,
        )
    except requests.RequestException:
        return []
    if not r.ok:
        return []
    return [
        c.get("committee_id") for c in (r.json().get("results") or [])
        if c.get("committee_id")
    ]


def _fetch_disbursements(committee_id: str, cycle: int) -> list[dict]:
    """Page through schedule_b for a committee within one two-year cycle.

    The schedule_b `candidate_id` filter returns earmarked transactions
    BENEFITING a candidate (heavily polluted by other committees'
    transfers); for actual outgoing spending we filter by committee_id of
    the candidate's principal committee.
    """
    out: list[dict] = []
    last_indexes: dict | None = None
    pages = 0
    while pages < MAX_PAGES_PER_OFFICIAL:
        params = {
            "committee_id": committee_id,
            "two_year_transaction_period": cycle,
            "per_page": 100,
            "sort": "-disbursement_amount",
            "api_key": FEC_API_KEY,
        }
        if last_indexes:
            # last_indexes keys already start with `last_` — pass through as-is
            for k, v in last_indexes.items():
                if v is not None:
                    params[k] = v
        try:
            r = requests.get(
                f"{FEC_BASE}/schedules/schedule_b/", params=params,
                timeout=HTTP_TIMEOUT, headers=UA,
            )
        except requests.RequestException as e:
            print(f"    [network error: {e}]")
            time.sleep(REQ_DELAY * 4)
            return out
        if r.status_code == 429:
            print("    [rate-limited, backing off 30s]")
            time.sleep(30)
            continue
        if not r.ok:
            print(f"    [HTTP {r.status_code}: {r.text[:120]}]")
            return out
        d = r.json()
        results = d.get("results") or []
        out.extend(results)
        pag = d.get("pagination") or {}
        last_indexes = pag.get("last_indexes")
        pages += 1
        time.sleep(REQ_DELAY)
        if not results or not last_indexes:
            break
    return out


def aggregate(rows: list[dict]) -> dict:
    by_payee: dict[str, float] = defaultdict(float)
    by_category: dict[str, float] = defaultdict(float)
    total = 0
    for r in rows:
        amt = _amt(r.get("disbursement_amount"))
        if amt <= 0:
            # Skip refunds, voided entries, and null amounts
            continue
        total += 1
        payee = (r.get("recipient_name") or "(unknown)").strip()[:120]
        by_payee[payee] += amt
        by_category[_bucket(r)] += amt

    top_payees = sorted(by_payee.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return {
        "top_payees": [{"name": n, "amount": round(a, 2)} for n, a in top_payees],
        "by_category": {k: round(v, 2) for k, v in by_category.items()},
        "total_transactions": total,
    }


def main() -> int:
    if not (SUPABASE_URL and SUPABASE_KEY):
        print("ERROR: SUPABASE_URL / SUPABASE_SERVICE_KEY missing", file=sys.stderr)
        return 1
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Pull federal officials with an fec_candidate_id
    eo = (
        supabase.table("elected_officials")
        .select("id, name")
        .eq("level", "federal")
        .execute()
    ).data or []
    eo_ids = [o["id"] for o in eo]
    cf = (
        supabase.table("campaign_finance")
        .select("official_id, fec_candidate_id, cycle")
        .in_("official_id", eo_ids)
        .not_.is_("fec_candidate_id", "null")
        .execute()
    ).data or []

    name_by_id = {o["id"]: o["name"] for o in eo}
    targets = sorted(cf, key=lambda x: name_by_id.get(x["official_id"], ""))

    print(f"Targets: {len(targets)} federal officials with FEC candidate IDs\n")

    ok = 0
    empty = 0
    errors = 0
    for i, t in enumerate(targets, 1):
        oid = t["official_id"]
        cand_id = t["fec_candidate_id"]
        cycle = t["cycle"]
        name = name_by_id.get(oid, "?")
        print(f"[{i}/{len(targets)}] {name} ({cand_id}, cycle {cycle})", flush=True)
        try:
            committees = _fetch_principal_committees(cand_id)
        except Exception as e:
            errors += 1
            print(f"    committee lookup failed: {e}")
            continue
        time.sleep(REQ_DELAY)
        if not committees:
            empty += 1
            print("    no principal committee found")
            continue
        rows: list[dict] = []
        for pcc in committees:
            try:
                rows.extend(_fetch_disbursements(pcc, cycle))
            except Exception as e:
                errors += 1
                print(f"    fetch failed for {pcc}: {e}")
        if not rows:
            empty += 1
            print(f"    no disbursements returned (committees: {committees})")
            continue
        agg = aggregate(rows)
        try:
            supabase.table("campaign_finance").update({
                "top_expenditures": agg,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }).eq("official_id", oid).eq("cycle", cycle).execute()
            ok += 1
            top = agg["top_payees"][:3]
            print(
                f"    fetched {len(rows)} rows, kept {agg['total_transactions']} positive | "
                f"buckets={list(agg['by_category'].keys())} | "
                f"top3={[(p['name'][:30], int(p['amount'])) for p in top]}"
            )
        except Exception as e:
            errors += 1
            print(f"    update failed: {e}")

    print("\n=== SUMMARY ===")
    print(f"  updated:  {ok}")
    print(f"  empty:    {empty}")
    print(f"  errors:   {errors}")
    print(f"  total:    {len(targets)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
