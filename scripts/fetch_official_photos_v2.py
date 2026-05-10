"""Replace ui-avatars.com placeholders on elected_officials with real photos.

Sources tried in order — falling back to a DiceBear professional silhouette
when nothing else hits. Sources 3/5/6/7 (Playwright-driven scrapes of
myfloridahouse / flsenate / fl-counties / floridaleagueofcities) and source
8 (Google CSE) are intentionally NOT implemented in-process — they require
heavy deps and a paid API key respectively. The script flags officials that
fall through to DiceBear with elected_officials.needs_photo=true so a future
Playwright pass can target them precisely.

Sources implemented (HTTP only):
  1. Congress Bioguide                     – federal officials
  2. Wikipedia REST API                    – named officials with a wiki page
  4. Ballotpedia infobox                   – any FL official with a BP page
  9. DiceBear personas (fallback)          – everyone else, flagged

Run:
  python scripts/fetch_official_photos_v2.py --limit 25 --dry-run
  python scripts/fetch_official_photos_v2.py --limit 100
  python scripts/fetch_official_photos_v2.py            # all 1,981

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY, CONGRESS_API_KEY (optional)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from typing import Optional, Tuple
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
CONGRESS_KEY = os.getenv("CONGRESS_API_KEY")

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "photos_log.txt")
NOTFOUND_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "photos_not_found.txt")

UA = "PolitiScore Photo Bot (+https://politiscore.com)"
TIMEOUT = 10
REQ_DELAY = 0.6  # respect rate limits everywhere
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
    try:
        r = session.head(url, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return False
        ctype = r.headers.get("content-type", "").lower()
        return ctype.startswith("image/")
    except requests.RequestException:
        return False


# ─── Source 1: Congress Bioguide ────────────────────────────────────────────
_FED_BIOGUIDE_CACHE: dict[str, str] = {}


def _load_fed_bioguide_cache() -> None:
    """One-shot fetch of all FL members of Congress with their bioguide IDs."""
    if not CONGRESS_KEY or _FED_BIOGUIDE_CACHE:
        return
    try:
        r = session.get(
            "https://api.congress.gov/v3/member",
            params={"stateCode": "FL", "limit": 250, "api_key": CONGRESS_KEY, "format": "json"},
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            log(f"  bioguide API returned {r.status_code}")
            return
        members = r.json().get("members", [])
        for m in members:
            name = (m.get("name") or "").strip()
            bid = (m.get("bioguideId") or "").strip()
            if name and bid:
                _FED_BIOGUIDE_CACHE[name.lower()] = bid
        log(f"  bioguide: cached {len(_FED_BIOGUIDE_CACHE)} FL members")
    except requests.RequestException as e:
        log(f"  bioguide cache load failed: {e}")


def try_bioguide(official: dict) -> Optional[str]:
    if (official.get("level") or "").lower() != "federal":
        return None
    _load_fed_bioguide_cache()
    name = (official.get("name") or "").strip()
    if not name:
        return None
    # API lists members as "Last, First", DB stores "First Last"; try both
    candidates = [name.lower()]
    parts = name.split()
    if len(parts) >= 2:
        candidates.append(f"{parts[-1]}, {' '.join(parts[:-1])}".lower())
    bid = next((_FED_BIOGUIDE_CACHE.get(c) for c in candidates if _FED_BIOGUIDE_CACHE.get(c)), None)
    if not bid:
        return None
    photo = f"https://bioguide.congress.gov/bioguide/photo/{bid[0]}/{bid}.jpg"
    return photo if head_is_image(photo) else None


# ─── Source 2: Wikipedia ────────────────────────────────────────────────────
def try_wikipedia(official: dict) -> Optional[str]:
    name = (official.get("name") or "").strip()
    if not name:
        return None
    slug = quote(name.replace(" ", "_"))
    try:
        r = session.get(f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}", timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get("type") == "disambiguation":
            return None
        thumb = (data.get("thumbnail") or {}).get("source")
        if not thumb:
            return None
        # Prefer the original (higher-res) over the thumb
        original = (data.get("originalimage") or {}).get("source") or thumb
        if not (original.startswith("http") and ("/wikipedia/" in original or "wikimedia.org" in original)):
            return None
        return original
    except requests.RequestException:
        return None


# ─── Source 4: Ballotpedia ──────────────────────────────────────────────────
def try_ballotpedia(official: dict) -> Optional[str]:
    name = (official.get("name") or "").strip()
    if not name:
        return None
    slug = quote(name.replace(" ", "_"))
    try:
        r = session.get(f"https://ballotpedia.org/{slug}", timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "lxml")
        # Most BP infoboxes are <div class="infobox-person"> or <table class="infobox">
        for parent in (
            soup.find("div", class_=re.compile(r"infobox", re.I)),
            soup.find("table", class_=re.compile(r"infobox", re.I)),
        ):
            if not parent:
                continue
            img = parent.find("img")
            if img and img.get("src"):
                src = img["src"]
                if src.startswith("//"):
                    src = "https:" + src
                if src.startswith("http") and head_is_image(src):
                    return src
    except requests.RequestException:
        return None
    return None


# ─── Source 9: DiceBear fallback ────────────────────────────────────────────
def dicebear_url(name: str) -> str:
    seed = quote((name or "anon").strip().lower().replace(" ", "-"))
    return f"https://api.dicebear.com/7.x/personas/svg?seed={seed}&backgroundColor=1a56db&radius=50"


# ─── Pipeline ───────────────────────────────────────────────────────────────
def find_photo(official: dict) -> Tuple[str, str, bool]:
    """Returns (url, source_label, is_real). Real means non-DiceBear."""
    for source_fn, label in [
        (try_bioguide,    "bioguide"),
        (try_wikipedia,   "wikipedia"),
        (try_ballotpedia, "ballotpedia"),
    ]:
        try:
            url = source_fn(official)
        except Exception as e:
            log(f"    {label} crashed: {e}")
            url = None
        time.sleep(REQ_DELAY)
        if url and len(url) < 500:
            return url, label, True
    return dicebear_url(official.get("name") or "anon"), "dicebear", False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Cap rows processed")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        log("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        return 1
    if not CONGRESS_KEY:
        log("WARN: CONGRESS_API_KEY not set — federal bioguide source will be skipped")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Pull all officials currently using ui-avatars
    rows: list[dict] = []
    page = 0
    PAGE = 500
    while True:
        q = (
            sb.table("elected_officials")
            .select("id, name, level, state, title, website, photo_url")
            .eq("state", "FL")
            .ilike("photo_url", "%ui-avatars%")
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
    if args.limit:
        rows = rows[: args.limit]
    log(f"to process: {len(rows)} ui-avatars officials  (dry-run={args.dry_run})")
    if not rows:
        return 0

    real_count = fallback_count = failed_count = 0
    notfound_lines: list[str] = []
    pending_updates: list[Tuple[int, str, bool]] = []  # (id, url, is_real)

    for i, off in enumerate(rows, 1):
        try:
            url, source, is_real = find_photo(off)
        except Exception as e:
            failed_count += 1
            log(f"[{i}/{len(rows)}] id={off['id']:5d} {off.get('name','?')[:32]:32}  CRASH {e}")
            continue
        if is_real:
            real_count += 1
        else:
            fallback_count += 1
            notfound_lines.append(f"{off['id']}\t{off.get('name','?')}\t{off.get('level','?')}\t{off.get('title','?')}")
        log(f"[{i}/{len(rows)}] id={off['id']:5d} {off.get('name','?')[:32]:32}  {source:11}  {url[:70]}")
        pending_updates.append((off["id"], url, is_real))

        # Batch UPDATE every 25 rows
        if len(pending_updates) >= 25 and not args.dry_run:
            for oid, u, real in pending_updates:
                sb.table("elected_officials").update({
                    "photo_url": u,
                    "needs_photo": (not real),
                    "updated_at": "now()",
                }).eq("id", oid).execute()
            pending_updates = []

    if pending_updates and not args.dry_run:
        for oid, u, real in pending_updates:
            sb.table("elected_officials").update({
                "photo_url": u,
                "needs_photo": (not real),
                "updated_at": "now()",
            }).eq("id", oid).execute()

    if notfound_lines:
        try:
            with open(NOTFOUND_FILE, "w", encoding="utf-8") as f:
                f.write("# Officials that fell through to the DiceBear fallback.\n")
                f.write("# id\tname\tlevel\ttitle\n")
                f.write("\n".join(notfound_lines))
        except OSError:
            pass

    log("")
    log(f"DONE  real={real_count}  fallback(dicebear)={fallback_count}  failed={failed_count}  of {len(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
