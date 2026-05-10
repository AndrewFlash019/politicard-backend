"""Second-pass backfill for the long tail of plain_english_summary nulls.

The v1 script left ~78 rows null because it filtered out anything that looked
like a refusal. This v2 explicitly asks Claude to respond with the literal
token "SKIP" when it can't write a faithful summary, so we leave those rows
null instead of inventing content.

Env: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "summaries_log.txt")
REQ_DELAY = 0.5
MAX_OUTPUT_TOKENS = 240


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()}  {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def build_prompt(row: dict) -> str:
    title = (row.get("title") or "").strip()[:600] or "(no title)"
    status = (row.get("status") or "").strip() or "(no status)"
    return (
        "Write a plain English 1-2 sentence summary for a constituent with no political "
        "background. Bill: "
        f"{title}. Status: {status}. "
        "Rules: factual, neutral, specific, no jargon. Start with what it does, not who "
        "introduced it. If you do not have enough information to write an accurate summary, "
        "respond with exactly: SKIP"
    )


def summarize(client, row: dict) -> str | None:
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
    parts = [b.text for b in (getattr(resp, "content", []) or []) if getattr(b, "type", None) == "text"]
    text = " ".join(parts).strip()
    if text.upper().startswith("SKIP"):
        return None
    return " ".join(text.split())[:600]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=78)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    missing = [k for k, v in [("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_SERVICE_KEY", SUPABASE_KEY), ("ANTHROPIC_API_KEY", ANTHROPIC_KEY)] if not v]
    if missing:
        log("ERROR missing env: " + ", ".join(missing))
        return 1

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    log(f"v2 start dry-run={args.dry_run} limit={args.limit} model={MODEL}")
    rows: list[dict] = []
    page = 0
    PAGE = 200
    while len(rows) < args.limit:
        q = (
            supabase.table("legislative_activity")
            .select("id, activity_type, bill_number, title, status")
            .is_("plain_english_summary", "null")
            .not_.is_("title", "null")
            .neq("title", "")
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
    rows = rows[: args.limit]
    log(f"fetched {len(rows)} rows missing summary")
    if not rows:
        return 0

    ok = skipped = failed = 0
    for i, row in enumerate(rows, 1):
        rid = row["id"]
        head = (row.get("bill_number") or row.get("activity_type") or "")[:24]
        summary = summarize(client, row)
        time.sleep(REQ_DELAY)
        if summary is None:
            skipped += 1
            log(f"[{i}/{len(rows)}] id={rid} {head}  SKIP")
            continue
        if args.dry_run:
            log(f"[{i}/{len(rows)}] id={rid} {head}  DRY  {summary[:120]}")
            continue
        try:
            supabase.table("legislative_activity").update({"plain_english_summary": summary}).eq("id", rid).execute()
            ok += 1
            log(f"[{i}/{len(rows)}] id={rid} {head}  OK   {summary[:100]}")
        except Exception as e:
            failed += 1
            log(f"[{i}/{len(rows)}] id={rid} {head}  UPDATE FAIL {e}")

    log(f"v2 done updated={ok} skipped={skipped} failed={failed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
