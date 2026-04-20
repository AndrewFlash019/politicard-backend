"""Backfill campaign_finance_spending for FL federal officials (cycle 2024).

Reads the set of officials with a known fec_candidate_id from the
existing campaign_finance table, fetches Schedule B disbursements for
each via app.services.fec_client.get_disbursements, and upserts the
aggregated totals/vendors/categories into campaign_finance_spending.

Usage: python scripts/populate_campaign_finance_spending.py

Requires env: SUPABASE_URL, SUPABASE_SERVICE_KEY, FEC_API_KEY.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import create_client

# Make `app.*` importable when running from the project root or scripts/.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.services.fec_client import get_disbursements  # noqa: E402

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
CYCLE = 2024


def load_officials(supabase) -> list[dict]:
    response = (
        supabase.table("campaign_finance")
        .select("official_id,official_name,fec_candidate_id")
        .eq("cycle", CYCLE)
        .not_.is_("fec_candidate_id", "null")
        .order("official_id")
        .execute()
    )
    return response.data or []


def upsert(supabase, row: dict) -> None:
    supabase.table("campaign_finance_spending").delete() \
        .eq("official_name", row["official_name"]) \
        .eq("cycle", row["cycle"]) \
        .execute()
    supabase.table("campaign_finance_spending").insert(row).execute()


def main() -> int:
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        return 1
    if os.getenv("FEC_API_KEY", "DEMO_KEY") == "DEMO_KEY":
        print("WARNING: using FEC DEMO_KEY (30 req/hr). Set FEC_API_KEY for speed.")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    officials = load_officials(supabase)
    if not officials:
        print("No officials with fec_candidate_id found — nothing to do.")
        return 0

    print(f"Loaded {len(officials)} officials with FEC candidate IDs.")
    ok = 0
    empty = 0
    failed: list[tuple[int, str, str]] = []
    started = time.time()

    for i, off in enumerate(officials, 1):
        oid = off["official_id"]
        name = off["official_name"]
        cand_id = off["fec_candidate_id"]
        print(f"\n[{i}/{len(officials)}] {name}  (id={oid}, fec={cand_id})", flush=True)

        try:
            agg = get_disbursements(cand_id, CYCLE)
        except Exception as e:
            failed.append((oid, name, f"exception: {e}"))
            print(f"  -> ERROR: {e}", flush=True)
            time.sleep(2)
            continue

        total_spent = agg.get("total_spent") or 0
        vendors = agg.get("top_vendors") or []
        categories = agg.get("spending_by_category") or []

        if total_spent <= 0 and not vendors and not categories:
            print(f"  -> empty (no 2024 filings for {cand_id})", flush=True)
            empty += 1
        else:
            print(
                f"  spent=${total_spent:,.0f}  vendors={len(vendors)}  "
                f"categories={len(categories)}",
                flush=True,
            )

        row = {
            "official_id": oid,
            "official_name": name,
            "fec_candidate_id": cand_id,
            "cycle": CYCLE,
            "total_spent": total_spent,
            "top_vendors": vendors,
            "spending_by_category": categories,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            upsert(supabase, row)
            ok += 1
            print(f"  -> upserted", flush=True)
        except Exception as e:
            failed.append((oid, name, f"upsert failed: {e}"))
            print(f"  -> ERROR upsert: {e}", flush=True)

        time.sleep(2)

    elapsed = time.time() - started
    print(f"\n{'=' * 60}")
    print(
        f"Done in {elapsed:.0f}s. Upserted: {ok}/{len(officials)}  "
        f"(empty={empty}, failed={len(failed)})"
    )
    if failed:
        print("Failures:")
        for oid, name, reason in failed:
            print(f"  {oid:>4}  {name:<32}  {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
