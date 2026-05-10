"""Backfill plain_english_summary for the rows that are still missing one.

Targeted at the long tail (~228 rows as of the overnight build) where
generate_plain_english_summaries.py left holes — typically rows whose title
is empty or whose activity_type wasn't covered by the default run.

Differs from the broader script:
  - shorter prompt (per spec)
  - 0.5s delay between calls
  - human-readable log to scripts/summaries_log.txt
  - bounded by --limit (default 250)

Env: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "summaries_log.txt")
REQ_DELAY = 0.5
MAX_OUTPUT_TOKENS = 220


def log(msg: str) -> None:
    line = f"{datetime.utcnow().isoformat()}Z  {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def build_prompt(row: dict) -> str:
    title = (row.get("title") or "").strip()[:600] or "(no title)"
    status = (row.get("status") or "").strip() or "(no status)"
    bill = (row.get("bill_number") or "").strip()
    bill_part = f" Bill number: {bill}." if bill else ""
    return (
        "Write a plain English summary in exactly 1-2 sentences for a constituent "
        "with no political background. Be factual, neutral, specific. Start with "
        "what the bill does, not who introduced it. Do not editorialize.\n\n"
        f"Bill: {title}.{bill_part} Status: {status}."
    )


_REFUSAL_PREFIXES = (
    "i don't", "i do not", "i'd need", "i would need",
    "i cannot", "i can't", "i'm unable", "i am unable",
    "without ", "there is no", "there's no",
)


def post_clean(text: str) -> str:
    if not text:
        return ""
    s = " ".join(text.strip().strip('"').split())
    low = s.lower()
    if any(low.startswith(p) for p in _REFUSAL_PREFIXES):
        return ""
    return s[:600]


def fetch_pending(supabase, *, limit: int) -> list[dict]:
    out: list[dict] = []
    page = 0
    PAGE = 500
    while len(out) < limit:
        q = (
            supabase.table("legislative_activity")
            .select("id, activity_type, bill_number, title, status, vote_position")
            .is_("plain_english_summary", "null")
            .order("date", desc=True)
            .range(page * PAGE, page * PAGE + PAGE - 1)
            .execute()
        )
        rows = q.data or []
        if not rows:
            break
        out.extend(rows)
        if len(rows) < PAGE:
            break
        page += 1
    return out[:limit]


def summarize(client, row: dict) -> Optional[str]:
    import anthropic
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=MAX_OUTPUT_TOKENS,
            messages=[{"role": "user", "content": build_prompt(row)}],
        )
    except anthropic.APIError as e:
        log(f"  anthropic error: {str(e)[:200]}")
        return None
    text_parts = [b.text for b in (getattr(resp, "content", []) or []) if getattr(b, "type", None) == "text"]
    return post_clean(" ".join(text_parts))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=250)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    missing = [k for k, v in [("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_SERVICE_KEY", SUPABASE_KEY), ("ANTHROPIC_API_KEY", ANTHROPIC_KEY)] if not v]
    if missing:
        log("ERROR missing env: " + ", ".join(missing))
        return 1

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    log(f"start dry-run={args.dry_run} limit={args.limit} model={MODEL}")
    rows = fetch_pending(supabase, limit=args.limit)
    log(f"fetched {len(rows)} rows missing summary")
    if not rows:
        return 0

    ok = 0; failed = 0; skipped = 0
    for i, row in enumerate(rows, 1):
        rid = row["id"]
        head = (row.get("bill_number") or row.get("activity_type") or "")[:24]
        summary = summarize(client, row)
        time.sleep(REQ_DELAY)
        if not summary:
            failed += 1
            log(f"[{i}/{len(rows)}] id={rid} {head}  FAIL")
            continue
        if args.dry_run:
            skipped += 1
            log(f"[{i}/{len(rows)}] id={rid} {head}  DRY  {summary[:120]}")
            continue
        try:
            supabase.table("legislative_activity").update({"plain_english_summary": summary}).eq("id", rid).execute()
            ok += 1
            log(f"[{i}/{len(rows)}] id={rid} {head}  OK   {summary[:100]}")
        except Exception as e:
            failed += 1
            log(f"[{i}/{len(rows)}] id={rid} {head}  UPDATE FAIL {e}")

    log(f"done updated={ok} dry={skipped} failed={failed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
