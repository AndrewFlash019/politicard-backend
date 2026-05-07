"""Backfill plain_english_summary on legislative_activity rows via Claude.

Targets rows where plain_english_summary IS NULL, builds a small prompt from
the bill_number/title/description/status, and writes a 1–2 sentence civic-
voter-friendly summary back. Idempotent: re-runs skip rows that already have
a summary.

Usage:
  python scripts/generate_plain_english_summaries.py
  python scripts/generate_plain_english_summaries.py --limit 500
  python scripts/generate_plain_english_summaries.py --activity-type bill_sponsored
  python scripts/generate_plain_english_summaries.py --dry-run --limit 5

Env: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")

REQ_DELAY = 0.25         # Anthropic SDK auto-retries 429s; modest spacing only
MAX_DESC_CHARS = 1200
MAX_TITLE_CHARS = 600
DEFAULT_ACTIVITY_TYPES = ("bill_sponsored", "bill_cosponsored", "vote")
DEFAULT_LIMIT = 100
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
MAX_OUTPUT_TOKENS = 200  # one short sentence — never need more


def _build_prompt(row: dict) -> str:
    """Per-row prompt: one paragraph asking Gemini for a 1–2 sentence summary
    aimed at a non-lawyer voter. The prompt explicitly forbids editorializing
    (no "important", "controversial", etc.) so the resulting text reads as
    neutral civic information."""
    activity = row.get("activity_type") or "legislation"
    bill = (row.get("bill_number") or "").strip()
    title = (row.get("title") or "").strip()[:MAX_TITLE_CHARS]
    desc = (row.get("description") or "").strip()[:MAX_DESC_CHARS]
    status = (row.get("status") or "").strip()
    chamber = (row.get("chamber") or "").strip()
    vote_pos = (row.get("vote_position") or "").strip()

    if activity == "vote":
        action = (
            f"This is a roll-call vote. The official voted {vote_pos} on the underlying motion."
            if vote_pos else "This is a roll-call vote on the underlying motion."
        )
    elif activity == "bill_cosponsored":
        action = "The official cosponsored this bill."
    else:
        action = "The official sponsored this bill."

    return (
        "You are a civic-news editor writing for ordinary voters. Read the bill or "
        "vote record below and write ONE plain-English sentence (max 240 characters) "
        "explaining what it does in concrete terms. Do not editorialize, do not use "
        "words like 'important', 'controversial', 'major', or 'historic'. Do not "
        "praise or criticize. Do not start with 'This bill', 'A bill', or the bill "
        "number. Just describe the action.\n\n"
        f"Activity type: {activity}\n"
        f"{action}\n"
        f"Chamber: {chamber}\n"
        f"Status: {status}\n"
        f"Bill number: {bill}\n"
        f"Title: {title}\n"
        f"Description: {desc}\n\n"
        "Write the one-sentence summary now (no quotes, no preamble):"
    )


_REFUSAL_PREFIXES = (
    "i don't", "i do not", "i'd need", "i would need",
    "i cannot", "i can't", "i'm unable", "i am unable",
    "without ", "there is no", "there's no",
)


def _post_clean(text: str) -> str:
    """Trim whitespace, strip trailing newlines, cap at 240 chars, and reject
    refusal-style outputs (Claude declines to summarize a row with insufficient
    info). Returns "" so the caller skips the row instead of writing a useless
    'I don't have enough information' string into the DB."""
    if not text:
        return ""
    s = text.strip().strip('"').strip()
    s = " ".join(s.split())
    low = s.lower()
    if any(low.startswith(p) for p in _REFUSAL_PREFIXES):
        return ""
    if len(s) > 240:
        cut = s[:240]
        last_period = cut.rfind(".")
        if last_period > 200:
            cut = cut[: last_period + 1]
        s = cut
    return s


def _fetch_pending(supabase, *, activity_types: list[str], limit: int) -> list[dict]:
    """Page through legislative_activity until we collect `limit` rows that
    still need a summary. Newest first."""
    out: list[dict] = []
    page = 0
    PAGE = 1000
    while len(out) < limit:
        q = (
            supabase.table("legislative_activity")
            .select("id, activity_type, bill_number, title, description, status, vote_position, chamber, date")
            .is_("plain_english_summary", "null")
            .in_("activity_type", activity_types)
            .not_.is_("title", "null")
            .neq("title", "")
            .neq("title", "(untitled)")
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


def _summarize_with_claude(client, row: dict) -> Optional[str]:
    """Call Claude Haiku for a one-sentence civic summary. The Anthropic SDK
    auto-retries 429s and 5xxs with exponential backoff (default max_retries=2),
    so we don't add our own loop — typed exceptions propagate up so the caller
    can skip the row and continue."""
    import anthropic
    prompt = _build_prompt(row)
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=MAX_OUTPUT_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.RateLimitError as e:
        print(f"    [rate-limited after retries: {str(e)[:120]}]", flush=True)
        return None
    except anthropic.APIError as e:
        print(f"    [anthropic error: {str(e)[:200]}]", flush=True)
        return None

    text_parts = []
    for block in getattr(resp, "content", []) or []:
        if getattr(block, "type", None) == "text":
            text_parts.append(block.text)
    return _post_clean(" ".join(text_parts))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Max rows to summarize this run (default {DEFAULT_LIMIT})")
    parser.add_argument("--activity-type", default=",".join(DEFAULT_ACTIVITY_TYPES),
                        help="Comma-separated activity types to target")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print summaries but do not write to DB")
    args = parser.parse_args()

    required = [
        ("SUPABASE_URL", SUPABASE_URL),
        ("SUPABASE_SERVICE_KEY", SUPABASE_KEY),
        ("ANTHROPIC_API_KEY", ANTHROPIC_KEY),
    ]
    missing = [k for k, v in required if not v]
    if missing:
        print("ERROR: missing env: " + ", ".join(missing), file=sys.stderr)
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    activity_types = [a.strip() for a in args.activity_type.split(",") if a.strip()]
    print(f"Fetching up to {args.limit} rows where plain_english_summary IS NULL "
          f"and activity_type IN {activity_types}…", flush=True)

    rows = _fetch_pending(supabase, activity_types=activity_types, limit=args.limit)
    print(f"Got {len(rows)} rows. dry-run={args.dry_run}, model={MODEL}, delay={REQ_DELAY}s\n", flush=True)

    if not rows:
        return 0

    ok = 0
    skipped = 0
    failed = 0
    for i, row in enumerate(rows, 1):
        rid = row["id"]
        head = (row.get("bill_number") or "")[:30].ljust(15)
        title_preview = (row.get("title") or "")[:60].replace("\n", " ")
        print(f"[{i}/{len(rows)}] id={rid} {head} {title_preview}", flush=True)

        summary = _summarize_with_claude(client, row)
        time.sleep(REQ_DELAY)

        if not summary:
            failed += 1
            print(f"    -> FAIL (no summary returned)", flush=True)
            continue

        if args.dry_run:
            print(f"    SUMMARY: {summary}", flush=True)
            skipped += 1
            continue

        try:
            supabase.table("legislative_activity") \
                .update({"plain_english_summary": summary}) \
                .eq("id", rid) \
                .execute()
            ok += 1
            print(f"    OK: {summary}", flush=True)
        except Exception as e:
            failed += 1
            print(f"    -> UPDATE FAIL {e}", flush=True)

    print(f"\nDONE. updated={ok} dry-run-skipped={skipped} failed={failed} of {len(rows)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
