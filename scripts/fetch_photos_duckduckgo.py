"""DuckDuckGo image search to backfill real photos for officials still on
DiceBear silhouettes (or anything flagged needs_photo=true).

For each candidate official:
  1. Build a query: "{name}" "{title}" "{county} Florida" official photo
  2. DDG image search, take results until one is from a trusted domain
     (any .gov, ballotpedia.org, or a small allow-list of FL outlets)
  3. HEAD-verify 200 + image/* content-type
  4. UPDATE elected_officials.photo_url + clear needs_photo

Run:
  python scripts/fetch_photos_duckduckgo.py --limit 5 --dry-run
  python scripts/fetch_photos_duckduckgo.py --limit 50
  python scripts/fetch_photos_duckduckgo.py                 # all
  python scripts/fetch_photos_duckduckgo.py --level=local

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (or SUPABASE_KEY)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from typing import Optional, Tuple
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from supabase import create_client

# duckduckgo_search was renamed to ddgs (DDG broke their old i.js endpoint).
# Prefer ddgs; fall back to duckduckgo_search so machines that still have
# the old name installed don't hard-fail on import.
try:
    from ddgs import DDGS  # type: ignore
except ImportError:
    import warnings
    warnings.filterwarnings("ignore", message=".*has been renamed to.*")
    from duckduckgo_search import DDGS  # type: ignore

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ddg_photos_log.txt")
NOTFOUND_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ddg_photos_not_found.txt")

UA = "PolitiScore Photo Bot (+https://politiscore.com)"
HEAD_TIMEOUT = 8
SEARCH_DELAY = 1.0   # per spec
MAX_RESULTS = 12
MAX_URL_LEN = 500

session = requests.Session()
session.headers.update({"User-Agent": UA})

# Trusted domain allow-list. Any *.gov is trusted; we also explicitly accept
# the FL state legislature properties, ballotpedia, and the major FL local
# outlets named in the spec.
EXACT_TRUSTED = {
    "ballotpedia.org",
    "myfloridahouse.gov",
    "flsenate.gov",
    "sun-sentinel.com",
    "miamiherald.com",
    "tampabay.com",
    "orlandosentinel.com",
}
SUFFIX_TRUSTED = (
    ".gov",
    ".gov.us",
    ".fl.us",
    ".myflorida.com",
)
# Specific FL counties we don't want to miss even if their site doesn't have a
# strict .gov suffix — every entry here is also doubled up by the .gov suffix
# rule when applicable.
EXACT_TRUSTED_LOCAL = {
    "broward.org",
    "miamidade.gov",
    "hillsboroughcounty.org",
    "pinellas.gov",
    "palmbeachcountyfl.gov",
    "orangecountyfl.net",
    "duvalcountyfl.gov",
    "polk-county.net",
    "brevardfl.gov",
    "volusia.org",
    "leeclerk.org",
    "alachuacounty.us",
    "marioncountyfl.org",
    "manateeclerk.com",
    "sccfl.gov",
}


def log(msg: str) -> None:
    print(msg, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except OSError:
        pass


def is_trusted(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    host = host.lower()
    if host in EXACT_TRUSTED or host in EXACT_TRUSTED_LOCAL:
        return True
    if any(host.endswith(suf) for suf in SUFFIX_TRUSTED):
        return True
    # Allow common WWW. variants of trusted hosts
    if host.startswith("www.") and host[4:] in EXACT_TRUSTED | EXACT_TRUSTED_LOCAL:
        return True
    return False


def head_is_image(url: str) -> bool:
    """HEAD → 200 + image/*. Some servers reject HEAD, so on 405 fall back to
    a tiny GET with Range and inspect content-type."""
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


def build_query(off: dict) -> str:
    name = (off.get("name") or "").strip()
    title = (off.get("title") or "").strip()
    county = (off.get("county") or "").strip()
    parts = [f'"{name}"']
    if title:
        parts.append(f'"{title}"')
    if county:
        parts.append(f'"{county} Florida"')
    parts.append("official photo")
    return " ".join(parts)


def find_photo(off: dict) -> Tuple[Optional[str], str]:
    """Return (image_url, host_label) or (None, 'no-trusted-result')."""
    query = build_query(off)
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(query, max_results=MAX_RESULTS, safesearch="off"))
    except Exception as e:
        return None, f"search-error: {str(e)[:80]}"

    for r in results:
        img = (r.get("image") or "").strip()
        page = (r.get("url") or "").strip()
        if not img or len(img) > MAX_URL_LEN:
            continue
        # Require the SOURCE page to be trusted (where the image lives).
        # Image CDNs (e.g. wp.com) are common, but if they're attached to a
        # trusted source page we accept them.
        if not (is_trusted(img) or is_trusted(page)):
            continue
        if not head_is_image(img):
            continue
        host = urlparse(img).hostname or "?"
        return img, host
    return None, "no-trusted-result"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Cap rows processed")
    parser.add_argument("--level", choices=["federal", "state", "local"], default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        log("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY required")
        return 1

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Pull candidates: needs_photo=true OR photo_url ILIKE '%dicebear%'
    rows: list[dict] = []
    page = 0
    PAGE = 500
    while True:
        q = (
            sb.table("elected_officials")
            .select("id, name, level, state, title, county, photo_url, needs_photo")
            .eq("state", "FL")
            .order("id")
            .range(page * PAGE, page * PAGE + PAGE - 1)
        )
        if args.level:
            q = q.eq("level", args.level)
        chunk = (q.execute().data or [])
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < PAGE:
            break
        page += 1

    candidates = [
        r for r in rows
        if r.get("needs_photo") is True
        or (r.get("photo_url") and "dicebear" in r["photo_url"])
    ]
    if args.limit:
        candidates = candidates[: args.limit]
    log(f"candidates: {len(candidates)}  level={args.level or 'all'}  dry-run={args.dry_run}")
    if not candidates:
        return 0

    # Dedup against existing real photos so DDG can't reuse the same city-
    # council card image for everyone in a row.
    existing_urls: set[str] = set()
    for r in rows:
        u = (r.get("photo_url") or "").strip()
        if u and "dicebear" not in u and "ui-avatars" not in u:
            existing_urls.add(u)

    real = miss = errors = 0
    pending: list[dict] = []
    misses: list[str] = []
    used_urls: set[str] = set()

    for i, off in enumerate(candidates, 1):
        url, label = find_photo(off)
        time.sleep(SEARCH_DELAY)

        if url and (url in existing_urls or url in used_urls):
            label = f"dup:{label[:30]}"
            url = None

        head = (off.get("name") or "?")[:30]
        if url:
            real += 1
            used_urls.add(url)
            log(f"[{i}/{len(candidates)}] id={off['id']:5d} {head:30} {label[:30]:30} HIT  {url[:80]}")
            pending.append({"id": off["id"], "url": url})
        else:
            if label.startswith("search-error"):
                errors += 1
            else:
                miss += 1
            misses.append(f"{off['id']}\t{off.get('name','?')}\t{off.get('level','?')}\t{off.get('title','?')}\t{label}")
            log(f"[{i}/{len(candidates)}] id={off['id']:5d} {head:30} {'-':30} MISS {label}")

        # Batch update every 25
        if not args.dry_run and len(pending) >= 25:
            for p in pending:
                sb.table("elected_officials").update({
                    "photo_url": p["url"],
                    "needs_photo": False,
                    "updated_at": "now()",
                }).eq("id", p["id"]).execute()
            pending = []

    if pending and not args.dry_run:
        for p in pending:
            sb.table("elected_officials").update({
                "photo_url": p["url"],
                "needs_photo": False,
                "updated_at": "now()",
            }).eq("id", p["id"]).execute()

    if misses:
        try:
            with open(NOTFOUND_FILE, "w", encoding="utf-8") as f:
                f.write("# id\tname\tlevel\ttitle\treason\n")
                f.write("\n".join(misses))
        except OSError:
            pass

    log("")
    log(f"DONE  hits={real}  miss={miss}  errors={errors}  of {len(candidates)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
