"""Scrape mayors + city councils for all 410 FL municipalities.

Strategy (fallback chain):
  1. Verified .gov / municipal website (URL pattern probe)
  2. FL League of Cities directory (https://www.flcities.com/find-a-city)
  3. MuniCode.com
  4. Wikipedia infobox

For each municipality:
  - Probe candidate URLs until one returns 200; persist the verified URL.
  - Walk a set of likely "government / council / mayor" paths.
  - Extract names attached to Mayor / Vice Mayor / Council Member / Commissioner
    phrasing. Capture phone / email / photo when present.
  - Insert into elected_officials with level='local', state='FL',
    category='City Government'.
  - Commit per-city so the run is crash-safe.

Usage:
    python scripts/scrape_florida_cities.py            # all unscraped cities
    python scripts/scrape_florida_cities.py --limit 20 # quick smoke test
    python scripts/scrape_florida_cities.py --only "Destin,Palm Coast"
    python scripts/scrape_florida_cities.py --rescrape # redo already-done cities

Requires: SUPABASE_URL, SUPABASE_SERVICE_KEY (or SUPABASE_KEY) in env.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.robotparser
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# --- Config ----------------------------------------------------------------

USER_AGENT = "PolitiScore Civic Data Bot +https://politiscore.com"
SAME_DOMAIN_DELAY = 3.0  # seconds
REQUEST_TIMEOUT = 12
URL_PROBE_TIMEOUT = 8
MAX_PAGE_BYTES = 2_500_000  # skip huge PDFs / binaries

URL_PATTERNS = [
    "https://www.cityof{slug}fl.gov",
    "https://www.{slug}fl.gov",
    "https://{slug}fl.gov",
    "https://www.{slug}.gov",
    "https://www.cityof{slug}.org",
    "https://www.townof{slug}fl.gov",
    "https://www.villageof{slug}fl.gov",
    "https://www.cityof{slug}.com",
    "https://www.{slug}.org",
]

OFFICIAL_PATHS = [
    "/",
    "/mayor",
    "/council",
    "/city-council",
    "/commissioners",
    "/city-commission",
    "/officials",
    "/elected-officials",
    "/government",
    "/about/council",
    "/government/mayor-and-council",
    "/government/city-council",
    "/your-government",
]

TITLE_PATTERNS = [
    ("Vice Mayor", re.compile(r"\bvice[-\s]*mayor\b", re.I)),
    ("Mayor", re.compile(r"\bmayor\b", re.I)),
    ("Council Member", re.compile(r"\b(?:council\s*member|councilmember|councilman|councilwoman|councilperson|council\s*representative)\b", re.I)),
    ("Commissioner", re.compile(r"\b(?:city\s*commissioner|commissioner)\b", re.I)),
]

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?(\d{3})\)?[\s.-]?(\d{3})[\s.-]?(\d{4})")
NAME_RE = re.compile(r"^[A-Z][A-Za-z'’.\-]+(?:\s+(?:[A-Z][A-Za-z'’.\-]+|[A-Z]\.?)){1,3}$")

# Any token that appears here disqualifies a candidate name (case-insensitive).
BAD_NAME_TOKENS = {
    "the", "and", "or", "of", "for", "with", "by", "about",
    "contact", "information", "email", "phone", "fax", "website",
    "address", "office", "hall", "hours", "meeting", "meetings",
    "agenda", "agendas", "minutes", "department", "departments",
    "services", "service", "city", "town", "village", "county",
    "mayor", "council", "commission", "commissioner", "member", "members",
    "elected", "officials", "official", "district", "ward", "seat",
    "government", "administration", "board", "committee", "committees",
    "home", "news", "events", "calendar", "welcome", "meet", "our", "your",
    "click", "here", "read", "more", "learn", "view", "see", "visit",
    "page", "staff", "directory", "biography", "bio",
    "term", "expires", "appointed", "elected",
}

NAME_STOPWORDS = {
    "City Hall", "City Council", "City Commission", "Elected Officials",
    "Home", "Government", "Contact Us", "Learn More", "Read More",
    "Board Of", "Office Of", "Mayor Pro Tem", "Mayor And Council",
}

# Link text / href substrings worth following one hop from the homepage.
GOV_LINK_KEYWORDS = [
    "mayor", "council", "commission", "elected", "officials",
    "government", "board-of", "our-government", "your-government",
    "city-hall", "leadership",
]

LOG = logging.getLogger("scrape_fl")


# --- Helpers ---------------------------------------------------------------


def slug(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[\s\-'’.,]+", "", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def avatar_url(name: str) -> str:
    return (
        "https://ui-avatars.com/api/?name="
        + urllib.parse.quote_plus(name)
        + "&size=256&background=1e40af&color=fff&bold=true"
    )


def normalize_phone(raw: str) -> str | None:
    m = PHONE_RE.search(raw)
    if not m:
        return None
    return f"({m.group(1)}) {m.group(2)}-{m.group(3)}"


def clean_name(raw: str) -> str | None:
    n = re.sub(r"\s+", " ", raw).strip(" ,.:;-|·•")
    n = re.sub(r"^(The\s+Honorable|Hon\.?|Mr\.?|Mrs\.?|Ms\.?|Dr\.?)\s+", "", n, flags=re.I)
    n = re.sub(r",?\s*(Esq\.?|Ph\.?D\.?|M\.?D\.?|Jr\.?|Sr\.?|II|III|IV)\.?$", "", n, flags=re.I)
    # Drop common leading label words before the actual name.
    n = re.sub(
        r"^(?:contact\s+information|contact|elected\s+officials?|biography|bio|meet|welcome)[\s:,\-]+",
        "",
        n,
        flags=re.I,
    )
    n = n.strip()
    if len(n) < 5 or len(n) > 60:
        return None
    if n in NAME_STOPWORDS:
        return None
    if not NAME_RE.match(n):
        return None
    tokens = [t.lower().strip(".") for t in n.split()]
    if any(t in BAD_NAME_TOKENS for t in tokens):
        return None
    if len(tokens) < 2:
        return None
    return n


# --- HTTP session with per-domain rate limiting ----------------------------


class PoliteSession:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.last_hit: dict[str, float] = {}
        self.robots_cache: dict[str, urllib.robotparser.RobotFileParser | None] = {}

    def _wait(self, host: str) -> None:
        last = self.last_hit.get(host)
        if last is not None:
            elapsed = time.time() - last
            if elapsed < SAME_DOMAIN_DELAY:
                time.sleep(SAME_DOMAIN_DELAY - elapsed)
        self.last_hit[host] = time.time()

    def _robots_ok(self, url: str) -> bool:
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc
        if host not in self.robots_cache:
            rp = urllib.robotparser.RobotFileParser()
            robots_url = f"{parsed.scheme}://{host}/robots.txt"
            try:
                r = self.session.get(robots_url, timeout=URL_PROBE_TIMEOUT)
                if r.status_code == 200:
                    rp.parse(r.text.splitlines())
                    self.robots_cache[host] = rp
                else:
                    self.robots_cache[host] = None
            except Exception:
                self.robots_cache[host] = None
        rp = self.robots_cache[host]
        if rp is None:
            return True
        try:
            return rp.can_fetch(USER_AGENT, url)
        except Exception:
            return True

    def get(self, url: str, timeout: float = REQUEST_TIMEOUT) -> requests.Response | None:
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc
        if not self._robots_ok(url):
            LOG.debug("robots.txt blocked %s", url)
            return None
        self._wait(host)
        try:
            r = self.session.get(url, timeout=timeout, allow_redirects=True, stream=True)
            # Bail on oversized responses (often PDFs we can't parse anyway).
            content = b""
            for chunk in r.iter_content(64 * 1024):
                content += chunk
                if len(content) > MAX_PAGE_BYTES:
                    break
            r._content = content
            return r
        except requests.RequestException as e:
            LOG.debug("GET %s failed: %s", url, e)
            return None


# --- URL discovery ---------------------------------------------------------


def discover_website(session: PoliteSession, muni: dict) -> str | None:
    name_slug = slug(muni["name"])
    if not name_slug:
        return None

    existing = (muni.get("website") or "").strip()
    candidates: list[str] = []
    if existing:
        candidates.append(existing.rstrip("/"))

    mtype = (muni.get("municipality_type") or "").lower()
    patterns = list(URL_PATTERNS)
    if "town" in mtype:
        patterns = ["https://www.townof{slug}fl.gov", "https://www.townof{slug}.org"] + patterns
    elif "village" in mtype:
        patterns = ["https://www.villageof{slug}fl.gov", "https://www.villageof{slug}.org"] + patterns

    for pat in patterns:
        candidates.append(pat.format(slug=name_slug))

    seen: set[str] = set()
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        r = session.get(url, timeout=URL_PROBE_TIMEOUT)
        if r is None:
            continue
        if 200 <= r.status_code < 400 and r.text and len(r.text) > 500:
            final = r.url.rstrip("/")
            LOG.info("  verified URL: %s", final)
            return final
    return None


# --- Extraction ------------------------------------------------------------


@dataclass
class Official:
    name: str
    role: str  # "Mayor" | "Vice Mayor" | "Council Member" | "Commissioner"
    phone: str | None = None
    email: str | None = None
    photo_url: str | None = None
    source_url: str = ""


@dataclass
class CityResult:
    officials: list[Official] = field(default_factory=list)
    source: str = ""
    verified_url: str | None = None

    def add(self, off: Official) -> None:
        key = (off.name.lower(), off.role)
        for existing in self.officials:
            if (existing.name.lower(), existing.role) == key:
                # prefer the record with more info
                if not existing.email and off.email:
                    existing.email = off.email
                if not existing.phone and off.phone:
                    existing.phone = off.phone
                if not existing.photo_url and off.photo_url:
                    existing.photo_url = off.photo_url
                return
        self.officials.append(off)


def role_from_title(title_text: str) -> str | None:
    for role, pat in TITLE_PATTERNS:
        if pat.search(title_text):
            return role
    return None


def extract_officials_from_html(html: str, source_url: str) -> list[Official]:
    """Heuristic extractor: looks for name-title pairings in likely layouts."""
    soup = BeautifulSoup(html, "lxml")

    # Strip noise. Keep <nav> / <header> since many municipal sites put council
    # member links (with role+name text) inside navigation blocks.
    for tag in soup(["script", "style", "form", "noscript"]):
        tag.decompose()

    found: list[Official] = []

    # Layout A: "Mayor John Smith" / "Council Member Jane Doe" in headings, cards,
    # list items, or anchor text (many CivicPlus-style sites use role+name anchors).
    for el in soup.find_all(["a", "h1", "h2", "h3", "h4", "h5", "strong", "b", "p", "li", "td", "span", "div"]):
        text = el.get_text(" ", strip=True)
        if not text or len(text) > 200:
            continue
        role = role_from_title(text)
        if not role:
            continue
        # Try to peel the role prefix off to get the name.
        stripped = re.sub(
            r"^(?:vice\s*mayor|mayor(?:\s*pro\s*tem)?|city\s*commissioner|commissioner|council\s*member|councilmember|councilman|councilwoman|councilperson)[\s:,\-]*",
            "",
            text,
            flags=re.I,
        ).strip()
        name = clean_name(stripped)
        if not name:
            # Try swapping order: "John Smith, Mayor"
            stripped2 = re.sub(
                r",?\s*(?:vice\s*mayor|mayor(?:\s*pro\s*tem)?|city\s*commissioner|commissioner|council\s*member|councilmember|councilman|councilwoman|councilperson).*$",
                "",
                text,
                flags=re.I,
            ).strip()
            name = clean_name(stripped2)
        if not name:
            continue

        # Collect contact info from nearby context.
        ctx_text = ""
        parent = el.parent
        if parent:
            ctx_text = parent.get_text(" ", strip=True)
        else:
            ctx_text = text
        email_m = EMAIL_RE.search(ctx_text)
        phone = normalize_phone(ctx_text)

        # photo: nearest <img> within the parent/card
        photo = None
        card = parent if parent else el
        img = card.find("img") if hasattr(card, "find") else None
        if img and img.get("src"):
            src = img["src"]
            photo = urllib.parse.urljoin(source_url, src)

        found.append(Official(
            name=name,
            role=role,
            phone=phone,
            email=email_m.group(0) if email_m else None,
            photo_url=photo,
            source_url=source_url,
        ))

    return found


def _is_html(r: requests.Response) -> bool:
    ct = (r.headers.get("content-type") or "").lower()
    if "text/html" in ct:
        return True
    return "<html" in r.text.lower()[:500]


def discover_gov_links(html: str, base: str) -> list[str]:
    """Find up to ~6 homepage links that plausibly lead to officials/council pages."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return []
    base_host = urllib.parse.urlparse(base).netloc
    found: list[tuple[int, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].split("#")[0]
        if not href or href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        absolute = urllib.parse.urljoin(base + "/", href)
        parsed = urllib.parse.urlparse(absolute)
        if parsed.netloc and parsed.netloc != base_host:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        text = a.get_text(" ", strip=True).lower()
        haystack = (text + " " + href.lower())
        score = 0
        for kw in GOV_LINK_KEYWORDS:
            if kw in haystack:
                score += 2 if kw in text else 1
        if score > 0:
            found.append((score, absolute))
    found.sort(key=lambda x: -x[0])
    return [u for _, u in found[:6]]


def scrape_city_site(session: PoliteSession, base_url: str, result: CityResult) -> None:
    tried: set[str] = set()

    def visit(url: str) -> None:
        if url in tried:
            return
        tried.add(url)
        r = session.get(url)
        if r is None or r.status_code >= 400 or not _is_html(r):
            return
        for off in extract_officials_from_html(r.text, r.url):
            result.add(off)

    # 1) Canned path probes.
    for path in OFFICIAL_PATHS:
        url = urllib.parse.urljoin(base_url + "/", path.lstrip("/"))
        visit(url)
        if any(o.role == "Mayor" for o in result.officials) and len(result.officials) >= 3:
            return

    # 2) One-hop crawl: pull candidate links off the homepage, follow top scorers.
    home = session.get(base_url)
    if home is not None and home.status_code < 400 and _is_html(home):
        for link in discover_gov_links(home.text, base_url):
            visit(link)
            if any(o.role == "Mayor" for o in result.officials) and len(result.officials) >= 3:
                return


# --- FLC / Wikipedia fallbacks --------------------------------------------


def fallback_flcities(session: PoliteSession, city_name: str, result: CityResult) -> None:
    # The FLC directory has individual profile pages. We search via a Google-style
    # query against their site.
    q = urllib.parse.quote_plus(f"{city_name} mayor")
    search = f"https://www.flcities.com/search?q={q}"
    r = session.get(search)
    if r is None or r.status_code >= 400:
        return
    soup = BeautifulSoup(r.text, "lxml")
    # Follow the first result that looks like a city profile.
    target = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/find-a-city/" in href or "flcities.com" in href:
            if city_name.lower().replace(" ", "-") in href.lower() or city_name.lower() in a.get_text(" ", strip=True).lower():
                target = urllib.parse.urljoin("https://www.flcities.com/", href)
                break
    if not target:
        return
    r2 = session.get(target)
    if r2 is None or r2.status_code >= 400:
        return
    officials = extract_officials_from_html(r2.text, r2.url)
    for off in officials:
        result.add(off)
    if officials and not result.source:
        result.source = "flcities"


def fallback_wikipedia(session: PoliteSession, city_name: str, county: str, result: CityResult) -> None:
    slug_wiki = city_name.replace(" ", "_") + ",_Florida"
    url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(slug_wiki)}"
    r = session.get(url)
    if r is None or r.status_code >= 400:
        return
    soup = BeautifulSoup(r.text, "lxml")
    box = soup.find("table", class_=re.compile(r"infobox"))
    if not box:
        return
    for row in box.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if not th or not td:
            continue
        label = th.get_text(" ", strip=True)
        value = td.get_text(" ", strip=True)
        role = None
        if re.search(r"\bmayor\b", label, re.I):
            role = "Mayor"
        elif re.search(r"\bvice\s*mayor\b", label, re.I):
            role = "Vice Mayor"
        if not role:
            continue
        # Wikipedia infoboxes often stuff party in parens: "John Smith (D)"
        name_raw = re.sub(r"\([^)]*\)", "", value).strip()
        name = clean_name(name_raw)
        if name:
            result.add(Official(name=name, role=role, source_url=url))
    if result.officials and not result.source:
        result.source = "wikipedia"


# --- DB I/O ----------------------------------------------------------------


def get_county_zips(supabase, county: str) -> str | None:
    try:
        r = (
            supabase.table("county_zips")
            .select("zip_codes")
            .eq("county", county)
            .limit(1)
            .execute()
        )
        if r.data:
            return r.data[0].get("zip_codes")
    except Exception as e:
        LOG.warning("county_zips lookup failed for %s: %s", county, e)
    return None


def existing_official_match(supabase, name: str, zip_codes: str | None) -> bool:
    """Return True if an official with this name already exists overlapping any zip."""
    try:
        r = (
            supabase.table("elected_officials")
            .select("id,name,zip_codes")
            .eq("level", "local")
            .eq("state", "FL")
            .ilike("name", name)
            .execute()
        )
    except Exception:
        return False
    if not r.data:
        return False
    if not zip_codes:
        return True  # same name + state + local == treat as dupe
    my_zips = {z.strip() for z in zip_codes.split(",") if z.strip()}
    for row in r.data:
        their = row.get("zip_codes") or ""
        their_zips = {z.strip() for z in their.split(",") if z.strip()}
        if my_zips & their_zips:
            return True
        if not their_zips:
            return True  # existing has no zips, assume same person
    return False


def title_for(muni_name: str, role: str) -> str:
    if role == "Mayor":
        return f"Mayor of {muni_name}"
    if role == "Vice Mayor":
        return f"Vice Mayor of {muni_name}"
    if role == "Council Member":
        return f"City Council Member, {muni_name}"
    if role == "Commissioner":
        return f"City Commissioner, {muni_name}"
    return role


def branch_for(role: str) -> str:
    return "executive" if role in ("Mayor", "Vice Mayor") else "legislative"


def insert_officials(supabase, muni: dict, result: CityResult, zip_codes: str | None) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    for off in result.officials:
        if existing_official_match(supabase, off.name, zip_codes):
            skipped += 1
            continue
        row = {
            "name": off.name,
            "title": title_for(muni["name"], off.role),
            "level": "local",
            "branch": branch_for(off.role),
            "state": "FL",
            "category": "City Government",
            "district": muni["name"],
            "zip_codes": zip_codes,
            "email": off.email,
            "phone": off.phone,
            "website": result.verified_url,
            "photo_url": off.photo_url or avatar_url(off.name),
            "party": None,
        }
        try:
            supabase.table("elected_officials").insert(row).execute()
            inserted += 1
        except Exception as e:
            LOG.warning("insert failed %s / %s: %s", muni["name"], off.name, e)
    return inserted, skipped


def log_failure(supabase, muni: dict, reason: str, url: str | None) -> None:
    try:
        supabase.table("scrape_failures").insert({
            "source_table": "fl_municipalities",
            "source_id": muni["id"],
            "identifier": f"{muni['name']}, {muni['county']}",
            "reason": reason[:500],
            "url": url,
        }).execute()
    except Exception as e:
        LOG.warning("scrape_failures insert failed: %s", e)


def mark_muni(
    supabase,
    muni_id: int,
    *,
    scraped_officials: bool,
    scraped_contact: bool,
    verified_url: str | None,
) -> None:
    patch: dict = {
        "scraped_officials": scraped_officials,
        "scraped_contact": scraped_contact,
        "last_scraped_at": datetime.now(timezone.utc).isoformat(),
    }
    if verified_url:
        patch["website"] = verified_url
    try:
        supabase.table("fl_municipalities").update(patch).eq("id", muni_id).execute()
    except Exception as e:
        LOG.warning("fl_municipalities update failed for id=%s: %s", muni_id, e)


# --- Main ------------------------------------------------------------------


def scrape_one(session: PoliteSession, supabase, muni: dict) -> tuple[int, int, str]:
    """Returns (inserted, skipped, status). status in {full,partial,fail}."""
    result = CityResult()

    verified = discover_website(session, muni)
    if verified:
        result.verified_url = verified
        result.source = "city_site"
        scrape_city_site(session, verified, result)

    # Fallback 1: FLC directory if nothing yet
    if not result.officials:
        try:
            fallback_flcities(session, muni["name"], result)
        except Exception as e:
            LOG.debug("flcities fallback error: %s", e)

    # Fallback 2: Wikipedia infobox (mayor only)
    if not any(o.role in ("Mayor", "Vice Mayor") for o in result.officials):
        try:
            fallback_wikipedia(session, muni["name"], muni["county"], result)
        except Exception as e:
            LOG.debug("wikipedia fallback error: %s", e)

    zip_codes = get_county_zips(supabase, muni["county"])
    inserted, skipped = 0, 0
    if result.officials:
        inserted, skipped = insert_officials(supabase, muni, result, zip_codes)

    any_contact = any(o.email or o.phone for o in result.officials if o.role == "Mayor")
    status = "fail"
    if result.officials:
        status = "full" if any(o.role == "Mayor" for o in result.officials) and len(result.officials) >= 3 else "partial"

    mark_muni(
        supabase,
        muni["id"],
        scraped_officials=bool(result.officials),
        scraped_contact=any_contact,
        verified_url=result.verified_url,
    )
    if not result.officials:
        log_failure(supabase, muni, "no officials extracted from any source", result.verified_url)

    return inserted, skipped, status


def load_munis(supabase, *, rescrape: bool, only: list[str] | None, limit: int | None) -> list[dict]:
    q = supabase.table("fl_municipalities").select(
        "id,name,county,municipality_type,website,scraped_officials"
    )
    if only:
        q = q.in_("name", only)
    elif not rescrape:
        q = q.eq("scraped_officials", False)
    q = q.order("name")
    if limit:
        q = q.limit(limit)
    r = q.execute()
    return r.data or []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--only", type=str, default=None, help="comma-separated city names")
    ap.add_argument("--rescrape", action="store_true")
    ap.add_argument("--log-file", type=str, default="scrape_florida_cities.log")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(args.log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
    if not url or not key:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_KEY) must be set")
        return 1

    supabase = create_client(url, key)
    only_list = [c.strip() for c in args.only.split(",")] if args.only else None
    munis = load_munis(supabase, rescrape=args.rescrape, only=only_list, limit=args.limit)

    LOG.info("Loaded %d municipalities to process", len(munis))
    session = PoliteSession()

    full = partial = fail = 0
    total_inserted = total_skipped = 0
    started = time.time()
    for i, muni in enumerate(munis, 1):
        LOG.info("[%d/%d] %s (%s)", i, len(munis), muni["name"], muni["county"])
        try:
            inserted, skipped, status = scrape_one(session, supabase, muni)
        except Exception as e:
            LOG.exception("unhandled error on %s: %s", muni["name"], e)
            try:
                log_failure(supabase, muni, f"unhandled: {e}", None)
                mark_muni(supabase, muni["id"], scraped_officials=False, scraped_contact=False, verified_url=None)
            except Exception:
                pass
            fail += 1
            continue
        total_inserted += inserted
        total_skipped += skipped
        if status == "full":
            full += 1
        elif status == "partial":
            partial += 1
        else:
            fail += 1
        LOG.info(
            "  -> status=%s inserted=%d skipped=%d  (totals: full=%d partial=%d fail=%d, officials=%d)",
            status, inserted, skipped, full, partial, fail, total_inserted,
        )

    elapsed = time.time() - started
    LOG.info("=" * 60)
    LOG.info(
        "Done in %.0fs. full=%d partial=%d fail=%d  officials inserted=%d skipped_dupes=%d",
        elapsed, full, partial, fail, total_inserted, total_skipped,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
