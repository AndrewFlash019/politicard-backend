"""Scrape JS-rendered municipal sites from `scrape_no_extraction_queue`.

Targets the ~120 FL cities whose static-HTML scrape (scrape_florida_cities.py)
returned no officials. Uses headless Playwright + Chromium to render the page,
follows the most promising "council / mayor / government" link, then asks
Claude Haiku for a JSON list of elected officials.

Usage:
    python scripts/scrape_js_cities.py --limit 2 --dry-run    # smoke test
    python scripts/scrape_js_cities.py --limit 5              # write 5 cities
    python scripts/scrape_js_cities.py                        # all queued

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY, ANTHROPIC_API_KEY (in .env).

Notes:
  - Model is claude-haiku-4-5-20251001 (latest haiku; 3-5 is retired).
  - Skip-list filters parking-page domains (hugedomains, squarespace, wix,
    bocachamber). Cities without a usable website fall back to URL probing.
  - One row per (name, title, district) — checked against existing
    elected_officials before insert.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")

MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
MAX_OUTPUT_TOKENS = 1500
PAGE_TIMEOUT_MS = 10_000
INTER_CITY_DELAY = 3.0
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PolitiScoreBot"
)

# Skip parking-pages, generic builder domains, and chamber-of-commerce sites.
BAD_DOMAIN_TOKENS = (
    "hugedomains.com",
    "bocachamber.com",
    "squarespace.com",
    "wix.com",
    "godaddy.com",
    "facebook.com",
    "weebly.com",
)

# A site is "official-looking" if its host ends in one of these or contains "city"/"town"/"village".
GOOD_TLD_HINTS = (".gov", ".org", ".us")
GOOD_HOST_TOKENS = ("city", "town", "village", "municipality")

# Link text fragments worth following from the homepage; ordered by preference.
COUNCIL_LINK_HINTS = [
    "city council",
    "council members",
    "elected officials",
    "city commission",
    "town council",
    "village council",
    "mayor",
    "commissioners",
    "government",
    "city hall",
    "about",
]

LOG = logging.getLogger("scrape_js_cities")


# --- Helpers ----------------------------------------------------------------


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower())


def is_blocked_url(url: str) -> bool:
    if not url:
        return True
    host = urlparse(url).netloc.lower()
    return any(token in host for token in BAD_DOMAIN_TOKENS)


def looks_official(url: str) -> bool:
    if not url:
        return False
    host = urlparse(url).netloc.lower()
    if any(host.endswith(t) for t in GOOD_TLD_HINTS):
        return True
    return any(token in host for token in GOOD_HOST_TOKENS)


def candidate_urls(city: str) -> list[str]:
    slug = slugify(city)
    if not slug:
        return []
    return [
        f"https://www.cityof{slug}.gov",
        f"https://{slug}fl.gov",
        f"https://www.{slug}fl.gov",
        f"https://www.{slug}.org",
        f"https://{slug}.org",
    ]


def probe_url(url: str, timeout: float = 5.0) -> bool:
    """HEAD/GET probe; True only on 2xx with a real response."""
    import requests  # local import; faster startup if you only do --dry-run
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers={"User-Agent": USER_AGENT})
        if 200 <= r.status_code < 300:
            return True
        # Some sites reject HEAD; fall back to a tiny GET.
        if r.status_code in (403, 405):
            r = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT}, stream=True)
            return 200 <= r.status_code < 300
    except Exception:
        return False
    return False


def resolve_website(supabase, city: str, county: str) -> Optional[str]:
    """Look up website in fl_municipalities; if missing/blocked, probe candidate URLs."""
    try:
        r = (
            supabase.table("fl_municipalities")
            .select("website")
            .eq("name", city)
            .eq("county", county)
            .limit(1)
            .execute()
        )
        rows = r.data or []
    except Exception as e:
        LOG.warning("fl_municipalities lookup failed for %s/%s: %s", city, county, e)
        rows = []

    site = (rows[0].get("website") if rows else None) or ""
    if site and not is_blocked_url(site) and looks_official(site):
        return site

    for u in candidate_urls(city):
        if probe_url(u):
            return u
    return None


# --- Playwright extraction --------------------------------------------------


@dataclass
class PageScrape:
    final_url: str
    text: str  # collapsed visible text
    error: Optional[str] = None


def render_page(playwright, url: str) -> PageScrape:
    """Render a URL with chromium, follow up to two council-shaped links, and
    return concatenated visible text from each page we touched.

    Many municipal sites have a /Government hub that links to a real /Council
    or /Commission roster page; one hop isn't enough. We collect text from
    each visited page so Claude can see whichever one carries the roster.

    Returns an error PageScrape if anything fails so the caller can continue.
    """
    browser = None
    visited: list[str] = []
    text_chunks: list[str] = []
    try:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()
        page.set_default_navigation_timeout(PAGE_TIMEOUT_MS)
        page.goto(url, wait_until="domcontentloaded")
        visited.append(page.url)
        text_chunks.append(_grab_body(page))

        for _ in range(2):  # up to two follow-up hops
            target = _best_council_link(page, exclude=set(visited))
            if not target:
                break
            try:
                page.goto(target, wait_until="domcontentloaded")
            except Exception as e:
                LOG.info("subpage nav failed (%s): %s", target, e)
                break
            visited.append(page.url)
            text_chunks.append(_grab_body(page))

        combined = _clean_text("\n\n".join(text_chunks))
        return PageScrape(final_url=page.url, text=combined)
    except Exception as e:
        return PageScrape(final_url=url, text="", error=str(e))
    finally:
        if browser:
            try:
                browser.close()
            except Exception:
                pass


def _grab_body(page) -> str:
    try:
        return page.evaluate("() => document.body && document.body.innerText || ''")
    except Exception:
        return ""


def _best_council_link(page, exclude: set[str] | None = None) -> Optional[str]:
    """Pick the highest-ranked link whose text matches a council-like hint.

    Skips already-visited URLs so a 2-hop traversal never bounces back to
    the homepage.
    """
    exclude = exclude or set()
    try:
        anchors = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => ({href: e.href, text: (e.innerText || '').trim().toLowerCase()}))",
        )
    except Exception:
        return None
    best_rank: Optional[int] = None
    best_url: Optional[str] = None
    for a in anchors or []:
        text = a.get("text") or ""
        href = a.get("href") or ""
        if not href or href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        full = urljoin(page.url, href).split("#", 1)[0]
        if full in exclude:
            continue
        for rank, hint in enumerate(COUNCIL_LINK_HINTS):
            if hint in text:
                if best_rank is None or rank < best_rank:
                    best_rank = rank
                    best_url = full
                break
    return best_url


def _clean_text(s: str) -> str:
    s = s or ""
    s = re.sub(r"\s+", " ", s).strip()
    # Cap at ~24KB to stay well under model context after we concatenate
    # text from multiple pages. Strips nav-bar repetition from long sites.
    return s[:24_000]


# --- Claude extraction ------------------------------------------------------


_EXTRACT_PROMPT = (
    "You will be given the visible text of a municipal government webpage. "
    "Extract every elected official mentioned (mayor, vice mayor, city council "
    "members, commissioners). Exclude appointed staff (city manager, clerk, "
    "attorney, police chief, etc.). Return a JSON array — and ONLY a JSON array "
    "— with this shape:\n"
    "[{\"name\": \"...\", \"title\": \"...\", \"email\": null, \"phone\": null}]\n"
    "Title must be one of: Mayor, Vice Mayor, Council Member, Commissioner. "
    "Use null for missing fields. If no elected officials are present, return [].\n\n"
    "Page text:\n"
)


def extract_officials(client, text: str) -> list[dict]:
    if not text.strip():
        return []
    msg = client.messages.create(
        model=MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        messages=[{"role": "user", "content": _EXTRACT_PROMPT + text}],
    )
    raw = ""
    for block in msg.content or []:
        if getattr(block, "type", None) == "text":
            raw += block.text
    raw = raw.strip()
    # Pull the JSON array even if the model wraps it in fences.
    m = re.search(r"\[[\s\S]*\]", raw)
    if not m:
        return []
    try:
        data = json.loads(m.group(0))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out = []
    for item in data:
        if not isinstance(item, dict):
            continue
        name = (item.get("name") or "").strip()
        title = (item.get("title") or "").strip()
        if not name or not title:
            continue
        if title not in {"Mayor", "Vice Mayor", "Council Member", "Commissioner"}:
            continue
        out.append({
            "name": name,
            "title": title,
            "email": (item.get("email") or None),
            "phone": (item.get("phone") or None),
        })
    return out


# --- DB writes --------------------------------------------------------------


def title_for(city: str, role: str) -> str:
    if role == "Mayor":
        return f"Mayor of {city}"
    if role == "Vice Mayor":
        return f"Vice Mayor of {city}"
    if role == "Commissioner":
        return f"{city} Commissioner"
    return f"{city} Council Member"


def branch_for(role: str) -> str:
    return "executive" if role in ("Mayor", "Vice Mayor") else "legislative"


def avatar_url(name: str) -> str:
    safe = re.sub(r"\s+", "+", (name or "Official").strip()) or "Official"
    return f"https://ui-avatars.com/api/?name={safe}&background=4a5d8c&color=fff&size=128"


def lookup_zip_codes(supabase, county: str) -> Optional[str]:
    try:
        r = (
            supabase.table("county_zips")
            .select("zip_codes")
            .eq("county", county)
            .limit(1)
            .execute()
        )
        rows = r.data or []
    except Exception as e:
        LOG.warning("county_zips lookup failed for %s: %s", county, e)
        return None
    return (rows[0].get("zip_codes") if rows else None) or None


def already_seated(supabase, name: str, title: str, county: str) -> bool:
    """Skip duplicates within (name, title) for cities in this county."""
    try:
        r = (
            supabase.table("elected_officials")
            .select("id")
            .eq("name", name)
            .eq("title", title)
            .eq("level", "local")
            .eq("state", "FL")
            .limit(1)
            .execute()
        )
        return bool(r.data)
    except Exception as e:
        LOG.warning("dedup lookup failed for %s/%s: %s", name, title, e)
        return False


def insert_official(supabase, *, city: str, county: str, role: str, official: dict, website: str, zip_codes: Optional[str], dry_run: bool) -> str:
    title = title_for(city, role)
    if already_seated(supabase, official["name"], title, county):
        return "skipped-duplicate"
    row = {
        "name": official["name"],
        "title": title,
        "level": "local",
        "branch": branch_for(role),
        "state": "FL",
        "category": "City Government",
        "district": city,
        "zip_codes": zip_codes,
        "email": official.get("email"),
        "phone": official.get("phone"),
        "website": website,
        "photo_url": avatar_url(official["name"]),
        "party": None,
    }
    if dry_run:
        LOG.info("[DRY-RUN] would insert: %s", row)
        return "dry-run"
    try:
        supabase.table("elected_officials").insert(row).execute()
        return "inserted"
    except Exception as e:
        LOG.warning("insert failed %s/%s: %s", city, official["name"], e)
        return "failed"


def mark_scraped(supabase, city: str, county: str, dry_run: bool) -> None:
    if dry_run:
        return
    now = datetime.now(timezone.utc).isoformat()
    try:
        (supabase.table("fl_municipalities")
            .update({"scraped_officials": True, "last_scraped_at": now})
            .eq("name", city).eq("county", county)
            .execute())
    except Exception as e:
        LOG.warning("fl_municipalities update failed for %s/%s: %s", city, county, e)
    try:
        (supabase.table("scrape_no_extraction_queue")
            .delete()
            .like("city_name", f"{city}, %")
            .execute())
    except Exception as e:
        LOG.warning("queue delete failed for %s: %s", city, e)


# --- Queue ------------------------------------------------------------------


def fetch_queue(supabase) -> list[tuple[str, str]]:
    """Return de-duplicated [(city, county)] from scrape_no_extraction_queue.

    The queue stores the combined "City, County" string; we split client-side
    because supabase-py's PostgREST builder doesn't expose split_part().
    """
    try:
        r = supabase.table("scrape_no_extraction_queue").select("city_name").execute()
        rows = r.data or []
    except Exception as e:
        LOG.error("queue fetch failed: %s", e)
        return []
    seen = set()
    out: list[tuple[str, str]] = []
    for row in rows:
        raw = (row.get("city_name") or "").strip()
        if "," not in raw:
            continue
        city, county = [p.strip() for p in raw.split(",", 1)]
        key = (city.lower(), county.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append((city, county))
    return out


# --- Main -------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="Max cities to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--only", type=str, default=None, help="Comma-separated city names")
    parser.add_argument("--start", type=int, default=0, help="Skip first N queue rows")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(os.path.dirname(__file__), "scrape_js_cities_log.txt"), encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    if not (SUPABASE_URL and SUPABASE_KEY and ANTHROPIC_KEY):
        LOG.error("Missing env: SUPABASE_URL, SUPABASE_SERVICE_KEY, ANTHROPIC_API_KEY required")
        return 2

    try:
        from supabase import create_client
        from anthropic import Anthropic
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        LOG.error("Missing dependency: %s. Run: pip install playwright anthropic supabase python-dotenv requests beautifulsoup4 && python -m playwright install chromium", e)
        return 2

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    claude = Anthropic(api_key=ANTHROPIC_KEY)

    queue = fetch_queue(supabase)
    LOG.info("queue contains %d unique (city, county) pairs", len(queue))

    if args.only:
        wanted = {c.strip().lower() for c in args.only.split(",") if c.strip()}
        queue = [(c, k) for (c, k) in queue if c.lower() in wanted]
    if args.start:
        queue = queue[args.start:]
    if args.limit is not None:
        queue = queue[: args.limit]

    LOG.info("processing %d cities (dry_run=%s)", len(queue), args.dry_run)

    counters = {"success": 0, "no_website": 0, "no_officials": 0, "render_failed": 0}
    inserted_total = 0

    with sync_playwright() as playwright:
        for idx, (city, county) in enumerate(queue, 1):
            LOG.info("[%d/%d] %s, %s", idx, len(queue), city, county)
            website = resolve_website(supabase, city, county)
            if not website:
                counters["no_website"] += 1
                LOG.info("  no usable website — skipping")
                time.sleep(INTER_CITY_DELAY)
                continue

            scrape = render_page(playwright, website)
            if scrape.error or not scrape.text:
                counters["render_failed"] += 1
                LOG.info("  render failed (%s): %s", website, scrape.error or "no text")
                time.sleep(INTER_CITY_DELAY)
                continue

            try:
                officials = extract_officials(claude, scrape.text)
            except Exception as e:
                LOG.warning("  Claude extraction error for %s: %s", city, e)
                officials = []

            if not officials:
                counters["no_officials"] += 1
                LOG.info("  no officials extracted from %s", scrape.final_url)
                time.sleep(INTER_CITY_DELAY)
                continue

            zip_codes = lookup_zip_codes(supabase, county)
            inserted_here = 0
            for off in officials:
                role = off["title"]  # already validated by extract_officials
                outcome = insert_official(
                    supabase, city=city, county=county, role=role,
                    official=off, website=website, zip_codes=zip_codes, dry_run=args.dry_run,
                )
                if outcome == "inserted":
                    inserted_here += 1
                LOG.info("    %s — %s (%s) → %s", off["name"], role, off.get("email") or "-", outcome)

            inserted_total += inserted_here
            counters["success"] += 1
            mark_scraped(supabase, city, county, args.dry_run)
            time.sleep(INTER_CITY_DELAY)

    LOG.info(
        "DONE — %d cities scraped, %d officials inserted (dry=%s). "
        "no_website=%d render_failed=%d no_officials=%d",
        counters["success"], inserted_total, args.dry_run,
        counters["no_website"], counters["render_failed"], counters["no_officials"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
