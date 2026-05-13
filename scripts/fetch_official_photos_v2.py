"""Backfill real photos via the MediaWiki pageimages API.

Targets every FL official whose photo_url is still a DiceBear silhouette
or whose needs_photo column is true. Single source — Wikipedia — so the
script is small, stateless, and free.

For each candidate:
  1. GET https://en.wikipedia.org/w/api.php?action=query&titles={name}
                                       &prop=pageimages&format=json&pithumbsize=300
  2. If a pageimages.thumbnail.source is returned, HEAD-verify image/*
  3. UPDATE photo_url + clear needs_photo
  4. Otherwise SKIP — never invent a photo

Batch updates every 10 rows, 0.5s delay between batches (Wikipedia is
rate-friendly; this is courteous, not strictly required).

Run:
  python scripts/fetch_official_photos_v2.py
  python scripts/fetch_official_photos_v2.py --limit 50
  python scripts/fetch_official_photos_v2.py --dry-run

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (or SUPABASE_KEY)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Optional

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wiki_photos_log.txt")
SKIP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wiki_photos_skipped.txt")

UA = "PolitiScore PhotoBot/1.0 (https://politiscore.com; ops@politiscore.com)"
HEAD_TIMEOUT = 8
WIKI_TIMEOUT = 10
PITHUMBSIZE = 300
BATCH_SIZE = 10
BATCH_DELAY = 0.5
MAX_URL_LEN = 500

session = requests.Session()
session.headers.update({"User-Agent": UA})


def log(msg: str) -> None:
    print(msg, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except OSError:
        pass


def head_is_image(url: str) -> bool:
    """HEAD → 200 + image/*. Falls back to a Range GET on 403/405 (some
    Wikimedia mirrors don't accept HEAD)."""
    try:
        r = session.head(url, timeout=HEAD_TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and r.headers.get("content-type", "").lower().startswith("image/"):
            return True
        if r.status_code in (403, 405):
            r2 = session.get(url, timeout=HEAD_TIMEOUT, headers={"Range": "bytes=0-32"}, stream=True)
            try:
                ok = r2.status_code in (200, 206) and r2.headers.get("content-type", "").lower().startswith("image/")
            finally:
                r2.close()
            return ok
        return False
    except requests.RequestException:
        return False


def wiki_thumbnail(name: str) -> Optional[str]:
    """Return a thumbnail URL via MediaWiki pageimages, or None when the
    article doesn't exist / has no associated image."""
    if not name:
        return None
    try:
        r = session.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "titles": name,
                "prop": "pageimages",
                "format": "json",
                "pithumbsize": PITHUMBSIZE,
                "redirects": 1,
            },
            timeout=WIKI_TIMEOUT,
        )
        if r.status_code != 200:
            return None
        pages = (r.json().get("query") or {}).get("pages") or {}
    except (requests.RequestException, ValueError):
        return None

    for _page_id, page in pages.items():
        thumb = (page.get("thumbnail") or {}).get("source")
        if not thumb or len(thumb) > MAX_URL_LEN:
            continue
        return thumb
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        log("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        return 1

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Pull candidates: needs_photo=true OR photo_url ILIKE '%dicebear%'
    rows: list[dict] = []
    page = 0
    PAGE = 500
    while True:
        chunk = (
            sb.table("elected_officials")
            .select("id, name, title, county, photo_url, needs_photo")
            .eq("state", "FL")
            .order("id")
            .range(page * PAGE, page * PAGE + PAGE - 1)
            .execute()
            .data
            or []
        )
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < PAGE:
            break
        page += 1

    candidates = [
        r for r in rows
        if r.get("needs_photo") is True
        or (r.get("photo_url") and "dicebear" in (r["photo_url"] or ""))
    ]
    if args.limit:
        candidates = candidates[: args.limit]

    # Cross-official dedup — never reuse a URL already attached to a real
    # photo elsewhere. Wikipedia is mostly fine here, but worth the cost.
    existing_urls: set[str] = set()
    for r in rows:
        u = (r.get("photo_url") or "").strip()
        if u and "dicebear" not in u and "ui-avatars" not in u:
            existing_urls.add(u)

    log(f"candidates: {len(candidates)}  dry-run={args.dry_run}")
    if not candidates:
        return 0

    success = 0
    skipped: list[str] = []
    pending: list[dict] = []

    for i, off in enumerate(candidates, 1):
        name = (off.get("name") or "").strip()
        thumb = wiki_thumbnail(name)
        if thumb and thumb in existing_urls:
            thumb = None  # de-dup
        if thumb and not head_is_image(thumb):
            thumb = None

        if thumb:
            success += 1
            existing_urls.add(thumb)
            log(f"[{i}/{len(candidates)}] SUCCESS {name} -> {thumb}")
            pending.append({"id": off["id"], "url": thumb})
        else:
            skipped.append(f"{off['id']}\t{name}\t{off.get('title','?')}\t{off.get('county','?')}")
            log(f"[{i}/{len(candidates)}] SKIP    {name}")

        # Batch flush every BATCH_SIZE rows; sleep between batches.
        if i % BATCH_SIZE == 0:
            if pending and not args.dry_run:
                for p in pending:
                    sb.table("elected_officials").update({
                        "photo_url": p["url"],
                        "needs_photo": False,
                        "updated_at": "now()",
                    }).eq("id", p["id"]).execute()
                pending = []
            time.sleep(BATCH_DELAY)

    # Final flush
    if pending and not args.dry_run:
        for p in pending:
            sb.table("elected_officials").update({
                "photo_url": p["url"],
                "needs_photo": False,
                "updated_at": "now()",
            }).eq("id", p["id"]).execute()

    if skipped:
        try:
            with open(SKIP_FILE, "w", encoding="utf-8") as f:
                f.write("# id\tname\ttitle\tcounty\n")
                f.write("\n".join(skipped))
        except OSError:
            pass

    log("")
    log(f"DONE  added={success}  skipped={len(skipped)}  of {len(candidates)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
