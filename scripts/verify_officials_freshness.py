"""Best-effort freshness check for FL county-level officials.

Visits the official government website on file for each row matching one of
the 2024 election positions (tax collector, sheriff, clerk of courts,
supervisor of elections, property appraiser, school board) and looks for the
DB-stored name string in the page HTML. If the name does NOT appear, the
record is flagged to scripts/stale_officials_report.csv for human review.

Does NOT auto-update the database — the goal is a triage list.

Run:
  python scripts/verify_officials_freshness.py
  python scripts/verify_officials_freshness.py --limit 50
  python scripts/verify_officials_freshness.py --titles "Sheriff,Tax Collector"
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from typing import Iterable

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

DEFAULT_TITLE_PATTERNS = (
    "tax collector", "sheriff", "clerk", "supervisor of elections",
    "property appraiser", "school board",
)
REPORT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "stale_officials_report.csv",
)
USER_AGENT = "PolitiScore Freshness Bot (+https://politiscore.com)"
REQ_TIMEOUT = 12
REQ_DELAY = 0.5


def _matches_position(title: str, patterns: Iterable[str]) -> bool:
    if not title:
        return False
    t = title.lower()
    return any(p in t for p in patterns)


def _fetch_text(url: str) -> str | None:
    if not url:
        return None
    if not url.startswith("http"):
        url = "https://" + url.lstrip("/")
    try:
        r = requests.get(url, timeout=REQ_TIMEOUT, headers={"User-Agent": USER_AGENT})
        if r.status_code != 200:
            return None
        return r.text or ""
    except requests.RequestException:
        return None


def _name_appears(name: str, html: str) -> str | None:
    """Return the matched form ("Full Name" / "Lastname, Firstname" / etc.) or
    None when not found in the page text."""
    if not name or not html:
        return None
    haystack = html.lower()
    full = name.strip().lower()
    if full in haystack:
        return name
    parts = name.strip().split()
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        rev = f"{last}, {first}".lower()
        if rev in haystack:
            return f"{last}, {first}"
        if last.lower() in haystack and first.lower() in haystack:
            return f"{first}…{last}"
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--titles", default=",".join(DEFAULT_TITLE_PATTERNS),
                        help="Comma-separated lowercase title substrings to include")
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set", file=sys.stderr)
        return 1

    patterns = tuple(p.strip().lower() for p in args.titles.split(",") if p.strip())

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    rows: list[dict] = []
    page = 0
    PAGE = 1000
    while True:
        q = (
            supabase.table("elected_officials")
            .select("id, name, title, website")
            .eq("state", "FL")
            .not_.is_("website", "null")
            .neq("website", "")
            .order("id")
            .range(page * PAGE, page * PAGE + PAGE - 1)
            .execute()
        )
        chunk = q.data or []
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < PAGE:
            break
        page += 1

    candidates = [r for r in rows if _matches_position(r["title"] or "", patterns)]
    if args.limit:
        candidates = candidates[: args.limit]

    print(f"checking {len(candidates)} officials against their websites…", flush=True)

    flagged: list[dict] = []
    for i, r in enumerate(candidates, 1):
        html = _fetch_text(r["website"])
        time.sleep(REQ_DELAY)
        if html is None:
            flagged.append({
                "id": r["id"], "current_name": r["name"], "found_name": "",
                "website": r["website"], "needs_review": "site unreachable",
            })
            continue
        match = _name_appears(r["name"] or "", html)
        if match is None:
            flagged.append({
                "id": r["id"], "current_name": r["name"], "found_name": "",
                "website": r["website"], "needs_review": "name not on landing page",
            })
        if i % 25 == 0:
            print(f"  scanned {i}/{len(candidates)}…", flush=True)

    with open(REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "current_name", "found_name", "website", "needs_review"])
        w.writeheader()
        for row in flagged:
            w.writerow(row)

    print(f"done. {len(flagged)} of {len(candidates)} officials flagged -> {REPORT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
