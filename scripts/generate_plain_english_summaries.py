"""Backfill plain_english_summary on legislative_activity rows via Gemini.

Targets rows where plain_english_summary IS NULL, builds a small prompt from
the bill_number/title/description/status, and writes a 1–2 sentence civic-
voter-friendly summary back. Idempotent: re-runs skip rows that already have
a summary.

Usage:
  python scripts/generate_plain_english_summaries.py
  python scripts/generate_plain_english_summaries.py --limit 500
  python scripts/generate_plain_english_summaries.py --activity-type bill_sponsored
  python scripts/generate_plain_english_summaries.py --dry-run --limit 5

Env: GOOGLE_AI_STUDIO_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY
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
GOOGLE_AI_KEY = os.getenv("GOOGLE_AI_STUDIO_API_KEY")

REQ_DELAY = 5.0          # 12 req/min, under free-tier 15 RPM ceiling
RETRY_MAX = 3
MAX_DESC_CHARS = 1200
MAX_TITLE_CHARS = 600
DEFAULT_ACTIVITY_TYPES = ("bill_sponsored", "bill_cosponsored", "vote")
DEFAULT_LIMIT = 100
MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-lite")


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


def _post_clean(text: str) -> str:
    """Trim whitespace, strip trailing newlines, and cap at 240 chars."""
    if not text:
        return ""
    s = text.strip().strip('"').strip()
    # Collapse internal whitespace
    s = " ".join(s.split())
    if len(s) > 240:
        # Cut at sentence boundary if possible
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


def _summarize_with_gemini(model, row: dict) -> Optional[str]:
    """Call Gemini with retry-on-429. Free tier is 15 RPM and ~1500 RPD on
    gemini-2.0-flash; rate-limit responses include a retry_delay seconds
    field that we honour."""
    prompt = _build_prompt(row)
    for attempt in range(1, RETRY_MAX + 1):
        try:
            resp = model.generate_content(prompt)
            text = getattr(resp, "text", None)
            if not text:
                cand = (getattr(resp, "candidates", None) or [None])[0]
                if cand and getattr(cand, "content", None):
                    parts = getattr(cand.content, "parts", []) or []
                    text = " ".join(getattr(p, "text", "") for p in parts).strip()
            return _post_clean(text or "")
        except Exception as e:
            msg = str(e)
            # Pull the retry_delay out of Gemini's quota error if present
            wait = None
            try:
                m = msg.split("retry_delay")[1] if "retry_delay" in msg else ""
                if "seconds" in m:
                    wait = int("".join(ch for ch in m.split("seconds:")[1].split("\n")[0] if ch.isdigit()))
            except Exception:
                wait = None
            if wait is None:
                wait = min(60 * attempt, 60)
            if "429" in msg or "ResourceExhausted" in msg or "quota" in msg.lower():
                print(f"    [rate-limited, sleeping {wait}s (attempt {attempt}/{RETRY_MAX})]", flush=True)
                time.sleep(wait)
                continue
            print(f"    [gemini error: {msg[:200]}]", flush=True)
            return None
    print(f"    [gave up after {RETRY_MAX} attempts]", flush=True)
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Max rows to summarize this run (default {DEFAULT_LIMIT})")
    parser.add_argument("--activity-type", default=",".join(DEFAULT_ACTIVITY_TYPES),
                        help="Comma-separated activity types to target")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print summaries but do not write to DB")
    args = parser.parse_args()

    missing = [v for k, v in [("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_SERVICE_KEY", SUPABASE_KEY), ("GOOGLE_AI_STUDIO_API_KEY", GOOGLE_AI_KEY)] if not v]
    if missing:
        print("ERROR: missing env: " + ", ".join(k for k, _ in [("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_SERVICE_KEY", SUPABASE_KEY), ("GOOGLE_AI_STUDIO_API_KEY", GOOGLE_AI_KEY)] if not _), file=sys.stderr)
        return 1

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    import google.generativeai as genai
    genai.configure(api_key=GOOGLE_AI_KEY)
    model = genai.GenerativeModel(MODEL)

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

        summary = _summarize_with_gemini(model, row)
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
