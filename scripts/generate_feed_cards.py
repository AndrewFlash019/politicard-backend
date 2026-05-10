"""Backfill feed_cards from legislative_activity vote rows.

We have ~25K legislative_activity rows but only ~4.5K feed_cards. Most votes
are not surfaced through /feed/{zip} structured sections. This script walks
every vote-type activity with a plain-English summary and inserts a matching
feed_card row when one does not already exist (matched on official_name +
bill_number + event_date).

Run:
  python scripts/generate_feed_cards.py            # backfill missing cards
  python scripts/generate_feed_cards.py --dry-run  # report only, no inserts

Env: DATABASE_URL (already used by the FastAPI app).

Notes:
  * feed_cards has no vote_position / result / official_id / state columns,
    so vote_position is encoded into the title ("[Official] voted YEA on Bill")
    and status from legislative_activity is stored in body alongside the
    plain-English summary.
  * county = NULL for federal-level votes (statewide visibility); for
    state/local rows we look up county from elected_officials.
  * dedup_key set so the existing feed_engine_daily.py UPSERT pipeline
    treats these rows the same as engine-generated cards.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("generate_feed_cards")


def vote_label(position: Optional[str]) -> str:
    if not position:
        return "voted on"
    p = position.lower().strip()
    if p.startswith("y") or p == "aye":
        return "voted YEA on"
    if p.startswith("n") and not p.startswith("not"):
        return "voted NAY on"
    if p in {"present", "abstain"}:
        return f"voted {p.upper()} on"
    return f"voted {position.upper()} on"


def build_title(official_name: str, vote_position: Optional[str], bill_number: Optional[str],
                summary: Optional[str]) -> str:
    label = vote_label(vote_position)
    bill_part = bill_number or "a bill"
    base = f"{official_name} {label} {bill_part}"
    if summary:
        excerpt = summary.strip()
        if len(excerpt) > 140:
            excerpt = excerpt[:140].rstrip() + "…"
        base = f"{base}: {excerpt}"
    return base[:200]


def build_body(summary: Optional[str], status: Optional[str]) -> str:
    parts: list[str] = []
    if summary:
        parts.append(summary.strip())
    if status:
        parts.append(f"Result: {status}")
    return "\n\n".join(parts) or ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report counts, do not insert")
    parser.add_argument("--limit", type=int, default=None, help="Cap rows scanned (debug)")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL not set in env")
        return 2

    engine = create_engine(db_url)

    with engine.begin() as conn:
        # elected_officials has no county column — federal+state senators and
        # reps get county=NULL (statewide reach). Local-level votes have no
        # natural county pivot here, so we skip them in the backfill.
        existing = {
            (row.official_name or "", row.bill_number or "", row.event_date)
            for row in conn.execute(text(
                "SELECT official_name, bill_number, event_date FROM feed_cards "
                "WHERE bill_number IS NOT NULL AND event_date IS NOT NULL"
            ))
        }
        log.info("loaded %d existing (name, bill, date) keys", len(existing))

        sql = (
            "SELECT id, official_id, official_name, official_level, bill_number, "
            "       title, status, vote_position, date, source, source_url, "
            "       full_text_url, plain_english_summary "
            "FROM legislative_activity "
            "WHERE activity_type = 'vote' "
            "  AND plain_english_summary IS NOT NULL "
            "  AND date IS NOT NULL "
            "ORDER BY id"
        )
        if args.limit:
            sql += f" LIMIT {int(args.limit)}"

        rows = conn.execute(text(sql)).fetchall()
        log.info("scanning %d vote rows", len(rows))

        inserted = 0
        skipped_existing = 0
        skipped_no_official_name = 0
        new_keys: set[tuple[str, str, object]] = set()

        for r in rows:
            official_name = (r.official_name or "").strip()
            if not official_name:
                skipped_no_official_name += 1
                continue
            bill_number = (r.bill_number or "").strip() or None
            key = (official_name, bill_number or "", r.date)
            if key in existing or key in new_keys:
                skipped_existing += 1
                continue
            new_keys.add(key)

            level = (r.official_level or "").lower() or None
            if level == "local":
                # No county pivot available; skip so we don't flood unrelated ZIPs
                continue
            county = None  # federal + state cards are statewide

            title = build_title(official_name, r.vote_position, bill_number,
                                r.plain_english_summary)
            body = build_body(r.plain_english_summary, r.status)
            source = r.source or "GovTrack.us"
            source_url = r.source_url or r.full_text_url
            dedup_key = f"vote:{official_name}:{bill_number or 'unknown'}:{r.date.isoformat()}"

            if args.dry_run:
                inserted += 1
                continue

            conn.execute(
                text(
                    """
                    INSERT INTO feed_cards (
                        card_type, title, body, icon, county, official_name,
                        official_level, source, source_url, priority, active,
                        event_date, bill_number, dedup_key, created_at, last_updated_at
                    ) VALUES (
                        'they_voted', :title, :body, :icon, :county, :name,
                        :level, :source, :source_url, 50, TRUE,
                        :event_date, :bill_number, :dedup_key, NOW(), NOW()
                    )
                    ON CONFLICT DO NOTHING
                    """
                ),
                {
                    "title": title,
                    "body": body,
                    "icon": "🗳️",
                    "county": county,
                    "name": official_name,
                    "level": level,
                    "source": source,
                    "source_url": source_url,
                    "event_date": r.date,
                    "bill_number": bill_number,
                    "dedup_key": dedup_key,
                },
            )
            inserted += 1
            if inserted % 500 == 0:
                log.info("  …inserted %d so far", inserted)

    log.info(
        "done: %s%d new feed_cards, %d already existed, %d skipped (no official_name)",
        "(dry-run) " if args.dry_run else "",
        inserted,
        skipped_existing,
        skipped_no_official_name,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
