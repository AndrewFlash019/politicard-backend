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
    # Extended patterns (often found via Wikipedia; still worth probing first).
    "https://www.{slug}fl.org",
    "https://{slug}fl.org",
    "https://www.{slug}fl.us",
    "https://www.cityof{slug}fl.us",
    "https://{hslug}-fl.gov",
    "https://www.{hslug}-fl.gov",
    "https://www.cityof{hslug}-fl.gov",
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
    "skip", "sidebar", "navigation", "menu", "main", "return",
    "last", "item", "search", "submit", "cancel",
    "navigate", "site", "sitemap", "content",
    "st", "ave", "rd", "dr", "blvd", "hwy", "lane", "way", "drive",
    "profile", "link", "view", "close", "open", "download", "print",
    "email", "tel", "http", "https", "www", "com", "org",
    "vice", "pro", "tem", "deputy", "acting", "interim", "honorable",
    "councilmember", "councilman", "councilwoman", "councilperson",
    "business", "impact", "estimate", "form", "forms", "request", "requests",
    "finance", "library", "parks", "departments", "records",
    "notice", "alert", "public",
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

# Path-part signals controlling whether "Mayor of {city}" extraction is allowed
# on a given URL. News/archive/history pages commonly mention former mayors by
# name — we refuse to treat those as the current mayor.
MAYOR_PATH_ALLOW_TOKENS = (
    "mayor", "office-of-the-mayor", "about-mayor", "about-the-mayor",
    "council", "commission", "government", "officials", "elected",
    "leadership", "city-hall", "town-hall", "about",
)
MAYOR_PATH_DENY_TOKENS = (
    "history", "histories", "historic", "news", "archive", "archives",
    "former", "past", "previous", "blog", "media", "press",
    "release", "event", "events", "agenda", "minutes", "meeting",
    "memoriam", "obituary", "calendar", "story", "stories",
)


def path_allows_mayor(url: str) -> bool:
    """True iff a URL's path is plausibly the *current* mayor's page."""
    try:
        path = urllib.parse.urlparse(url).path.lower()
    except Exception:
        return True
    for bad in MAYOR_PATH_DENY_TOKENS:
        if bad in path:
            return False
    if path in ("", "/"):
        return True  # homepage is fine
    for good in MAYOR_PATH_ALLOW_TOKENS:
        if good in path:
            return True
    return False

LOG = logging.getLogger("scrape_fl")


# --- Helpers ---------------------------------------------------------------


def slug(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[\s\-'’.,]+", "", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def hyphen_slug(name: str) -> str:
    """Lowercase with internal hyphens kept, external punctuation stripped."""
    s = name.lower()
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"[^a-z0-9\-]", "", s)
    s = re.sub(r"-+", "-", s).strip("-")
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
    # Normalize ALL-CAPS to Title Case so NAME_RE accepts them.
    if n.isupper() or (n.replace(" ", "").isupper() and not any(c.islower() for c in n)):
        parts = []
        for word in n.split():
            if re.fullmatch(r"[A-Z]\.?", word):
                parts.append(word)  # initial
            else:
                parts.append(word[:1] + word[1:].lower())
        n = " ".join(parts)
    if not NAME_RE.match(n):
        return None
    tokens = [t.lower().strip(".") for t in n.split()]
    if any(t in BAD_NAME_TOKENS for t in tokens):
        return None
    if len(tokens) < 2:
        return None
    # Length caps: real names are typically 2-3 full words plus maybe one
    # middle initial. Anything larger is almost always a concatenation of two
    # separate names ("Whitman Daniel J Alfonso") or harvested form label.
    full_words = [t for t in n.split() if not re.fullmatch(r"[A-Z]\.?", t)]
    if len(full_words) > 3 or len(tokens) > 4:
        return None
    if len(tokens) == 4:
        # 4 tokens only allowed if exactly one is a middle initial in position 1
        # ("Scott J. Brook Jr."-style suffix is stripped above already, so this
        # usually catches "Whitman Daniel J Alfonso").
        return None
    # Reject domain-like tokens ("MyClearwater.com") and hyphenated junk.
    for t in n.split():
        if "." in t[1:] and not re.fullmatch(r"[A-Z]\.", t):
            return None
        if t.count("-") > 1:
            return None
    # Reject if the same full-word appears twice ("Scott Black Scott Black",
    # "Councilmember Councilmember").
    lowered = [t.lower().strip(".") for t in full_words]
    if len(set(lowered)) < len(lowered):
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


def _looks_like_city_site(html: str, city_name: str, url: str = "") -> bool:
    """Cheap heuristic: does this page actually belong to the given city?"""
    if not html:
        return False
    cname = city_name.lower()
    lower = html.lower()
    # The city's name must appear at all in the doc.
    if cname not in lower:
        return False
    # Strong signal: city name appears in the <title>.
    title_m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    title = (title_m.group(1) if title_m else "").lower()
    if cname in title:
        return True
    # Strong signal: URL is on a .gov domain and city name is in the HTML.
    host = urllib.parse.urlparse(url).netloc.lower()
    if host.endswith(".gov"):
        return True
    # Otherwise require a civic-context phrase somewhere in the first 80k.
    window = lower[:80_000]
    for phrase in (
        f"city of {cname}", f"town of {cname}", f"village of {cname}",
        "city council", "city commission", "town council",
        "mayor", "elected officials", "city hall", "town hall",
        "municipal", "city clerk", "city manager",
    ):
        if phrase in window:
            return True
    return False


def discover_website(session: PoliteSession, muni: dict) -> str | None:
    name_slug = slug(muni["name"])
    if not name_slug:
        return None
    name_hslug = hyphen_slug(muni["name"])

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
        try:
            candidates.append(pat.format(slug=name_slug, hslug=name_hslug))
        except KeyError:
            candidates.append(pat.format(slug=name_slug))

    seen: set[str] = set()
    fallback_url: str | None = None  # first-reachable-but-unverified, used only if no real match
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        r = session.get(url, timeout=URL_PROBE_TIMEOUT)
        if r is None:
            continue
        if not (200 <= r.status_code < 400 and r.text and len(r.text) > 500):
            continue
        final = r.url.rstrip("/")
        if _looks_like_city_site(r.text, muni["name"], final):
            LOG.info("  verified URL: %s", final)
            return final
        if fallback_url is None and (".gov" in urllib.parse.urlparse(final).netloc):
            # A .gov URL that didn't prove its identity is still better than nothing.
            fallback_url = final

    # Last resort: pull the official website from the Wikipedia article's infobox.
    wiki_url = discover_website_via_wikipedia(session, muni["name"])
    if wiki_url:
        r = session.get(wiki_url, timeout=URL_PROBE_TIMEOUT)
        if r is not None and 200 <= r.status_code < 400 and r.text:
            final = r.url.rstrip("/")
            if _looks_like_city_site(r.text, muni["name"], final):
                LOG.info("  verified URL (via wikipedia): %s", final)
                return final
            if fallback_url is None:
                fallback_url = final
                LOG.info("  tentative URL (via wikipedia): %s", final)

    if fallback_url and fallback_url != (wiki_url or "").rstrip("/"):
        LOG.info("  tentative URL: %s", fallback_url)
    return fallback_url


def discover_website_via_wikipedia(session: PoliteSession, city_name: str) -> str | None:
    """Pull the `website` field from the city's Wikipedia infobox."""
    wiki_slug = city_name.replace(" ", "_") + ",_Florida"
    url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(wiki_slug)}"
    r = session.get(url, timeout=URL_PROBE_TIMEOUT)
    if r is None or r.status_code >= 400 or not r.text:
        return None
    try:
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        return None
    box = soup.find("table", class_=re.compile(r"infobox"))
    if not box:
        return None
    for row in box.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if not th or not td:
            continue
        if re.search(r"website", th.get_text(" ", strip=True), re.I):
            a = td.find("a", href=True)
            if a and a["href"].startswith(("http://", "https://")):
                return a["href"].rstrip("/")
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
    # Secondary pool of mayor candidates surfaced on non-authoritative pages
    # (news/history mentions, ambiguous matches). Not emitted unless the
    # primary pool is empty, and even then only if there's exactly one.
    mayor_candidates: list[Official] = field(default_factory=list)
    rejected_mayors: list[Official] = field(default_factory=list)
    source: str = ""
    verified_url: str | None = None

    def add(self, off: Official, *, mayor_allowed: bool = True) -> None:
        if off.role in ("Mayor", "Vice Mayor") and not mayor_allowed:
            # Keep for debugging / AMBIGUOUS_MAYOR logs but don't emit.
            self.mayor_candidates.append(off)
            return
        key = (off.name.lower(), off.role)
        for existing in self.officials:
            if (existing.name.lower(), existing.role) == key:
                if not existing.email and off.email:
                    existing.email = off.email
                if not existing.phone and off.phone:
                    existing.phone = off.phone
                if not existing.photo_url and off.photo_url:
                    existing.photo_url = off.photo_url
                return
        self.officials.append(off)

    def finalize_mayor(self) -> None:
        """Enforce 'at most one Mayor of X' — if multiple Mayor records came in,
        keep the first (highest-signal, inserted earliest) and demote the rest
        to rejected_mayors for AMBIGUOUS_MAYOR reporting."""
        mayors = [o for o in self.officials if o.role == "Mayor"]
        if len(mayors) <= 1:
            return
        keeper = mayors[0]
        rejects = mayors[1:]
        self.officials = [o for o in self.officials if o.role != "Mayor" or o is keeper]
        self.rejected_mayors.extend(rejects)


def role_from_title(title_text: str) -> str | None:
    for role, pat in TITLE_PATTERNS:
        if pat.search(title_text):
            return role
    return None


# Direct text patterns. Name is 2-4 capitalized words (allowing an initial),
# role is any of mayor/vice mayor/council member/commissioner variants.
_NAME_TOKEN = r"[A-Z][A-Za-z'’.\-]+"
_NAME_CHUNK = rf"(?:{_NAME_TOKEN}(?:\s+(?:{_NAME_TOKEN}|[A-Z]\.?)){{1,3}})"
_ROLE_TOKEN = r"(?:vice\s*mayor|mayor\s*pro\s*tem|mayor|city\s*commissioner|commissioner|council\s*member|councilmember|councilman|councilwoman|councilperson)"
ROLE_THEN_NAME_RE = re.compile(rf"\b{_ROLE_TOKEN}[:\s]+({_NAME_CHUNK})\b", re.I)
NAME_THEN_ROLE_RE = re.compile(rf"\b({_NAME_CHUNK})\s*[,\-–]?\s*{_ROLE_TOKEN}\b", re.I)


def extract_from_text(text: str, source_url: str) -> list[Official]:
    """Scan free text for role+name and name+role patterns."""
    out: list[Official] = []
    seen: set[tuple[str, str]] = set()

    def _emit(raw_name: str, role_text: str) -> None:
        role = role_from_title(role_text)
        if not role:
            return
        name = clean_name(raw_name)
        if not name:
            return
        key = (name.lower(), role)
        if key in seen:
            return
        seen.add(key)
        out.append(Official(name=name, role=role, source_url=source_url))

    for m in ROLE_THEN_NAME_RE.finditer(text):
        _emit(m.group(1), m.group(0))
    for m in NAME_THEN_ROLE_RE.finditer(text):
        _emit(m.group(1), m.group(0))
    return out


def _local_card_text(el, max_chars: int = 600) -> str:
    """Return text from the smallest plausible bio-card scope around `el`:
    either the element itself, or the nearest ancestor that also holds an
    <img>/<h*> (typical profile card). Bounded so we don't scoop the whole page.
    """
    try:
        own = el.get_text(" ", strip=True)
        if own and len(own) <= max_chars:
            best = own
        else:
            best = (own or "")[:max_chars]
        # Walk up to 2 ancestors; stop at the first one that looks card-shaped
        # (contains an <img> or an <h1-5>) and is still modest in size.
        cur = el.parent
        for _ in range(2):
            if cur is None or not hasattr(cur, "find"):
                break
            ctext = cur.get_text(" ", strip=True)
            if len(ctext) > max_chars:
                break
            if cur.find("img") or cur.find(["h1", "h2", "h3", "h4", "h5"]):
                best = ctext
                break
            cur = cur.parent
        return best
    except Exception:
        return ""


def extract_officials_from_html(html: str, source_url: str, *, mayor_allowed: bool = True) -> list[Official]:
    """Heuristic extractor: looks for name-title pairings in likely layouts."""
    soup = BeautifulSoup(html, "lxml")

    # Strip noise. Keep <nav> / <header> (many municipal sites put council
    # member links inside nav blocks) and keep <form> (ASP.NET/CivicPlus sites
    # wrap their entire page body inside a postback form).
    for tag in soup(["script", "style", "noscript"]):
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

        if role in ("Mayor", "Vice Mayor") and not mayor_allowed:
            # Page path isn't authoritative for the current mayor — skip.
            continue

        # Collect contact info from the *local* card scope only, so a single
        # generic "cityhall@..." email doesn't get broadcast to every official.
        ctx_text = _local_card_text(el)
        email_m = EMAIL_RE.search(ctx_text) if ctx_text else None
        phone = normalize_phone(ctx_text) if ctx_text else None

        # photo: nearest <img> within the parent/card
        photo = None
        card = el.parent if el.parent else el
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

    # Layout B: scan the whole page text for Role+Name / Name+Role patterns.
    # Catches sites that dump member bios into a single <div> or paragraph.
    page_text = soup.get_text(" ", strip=True)
    for off in extract_from_text(page_text, source_url):
        if off.role in ("Mayor", "Vice Mayor") and not mayor_allowed:
            continue
        if not any(o.name.lower() == off.name.lower() and o.role == off.role for o in found):
            found.append(off)

    # Safety net: don't let a single email get assigned to the entire council.
    # If the page has exactly one distinct email and ≥2 officials carry it,
    # keep it only on the mayor and NULL it for everyone else.
    emails = [(i, o.email) for i, o in enumerate(found) if o.email]
    if emails:
        distinct = {e.lower() for _, e in emails}
        if len(distinct) == 1 and len(emails) >= 2:
            the_email = emails[0][1]
            keeper_idx = None
            for i, o in enumerate(found):
                if o.email and o.email.lower() == the_email.lower() and o.role == "Mayor":
                    keeper_idx = i
                    break
            if keeper_idx is None:
                # No mayor in the group — drop the shared email entirely.
                for _, o in enumerate(found):
                    if o.email and o.email.lower() == the_email.lower():
                        o.email = None
            else:
                for i, o in enumerate(found):
                    if i != keeper_idx and o.email and o.email.lower() == the_email.lower():
                        o.email = None

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
        allow_mayor = path_allows_mayor(r.url)
        for off in extract_officials_from_html(r.text, r.url, mayor_allowed=allow_mayor):
            result.add(off, mayor_allowed=allow_mayor)

    # 1) Canned path probes.
    for path in OFFICIAL_PATHS:
        url = urllib.parse.urljoin(base_url + "/", path.lstrip("/"))
        visit(url)
        if any(o.role == "Mayor" for o in result.officials) and len(result.officials) >= 3:
            return

    # 2) One-hop crawl: pull candidate links off the homepage.
    second_hop_seeds: list[tuple[str, str]] = []  # (html, url) of pages to mine for further links
    home = session.get(base_url)
    if home is not None and home.status_code < 400 and _is_html(home):
        for link in discover_gov_links(home.text, base_url):
            if link in tried:
                continue
            tried.add(link)
            r = session.get(link)
            if r is None or r.status_code >= 400 or not _is_html(r):
                continue
            offs_before = len(result.officials)
            allow_mayor = path_allows_mayor(r.url)
            for off in extract_officials_from_html(r.text, r.url, mayor_allowed=allow_mayor):
                result.add(off, mayor_allowed=allow_mayor)
            # If this page didn't yield anyone, keep its HTML as a candidate for 2-hop.
            if len(result.officials) == offs_before:
                second_hop_seeds.append((r.text, r.url))
            if any(o.role == "Mayor" for o in result.officials) and len(result.officials) >= 3:
                return

    # 3) Two-hop crawl: any intermediate government index page we visited had no
    # officials — pull gov-keyword links off those and try those too. This
    # handles CivicPlus-style sites where "Government" → "City Council" is two
    # clicks deep (Casselberry, Chipley, etc).
    if not any(o.role == "Mayor" for o in result.officials):
        seen_links: set[str] = set()
        for html, src_url in second_hop_seeds[:3]:
            for link in discover_gov_links(html, src_url):
                if link in tried or link in seen_links:
                    continue
                seen_links.add(link)
                tried.add(link)
                r = session.get(link)
                if r is None or r.status_code >= 400 or not _is_html(r):
                    continue
                allow_mayor = path_allows_mayor(r.url)
                for off in extract_officials_from_html(r.text, r.url, mayor_allowed=allow_mayor):
                    result.add(off, mayor_allowed=allow_mayor)
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


def _db_has_mayor(supabase, city_name: str) -> bool:
    """True if the DB already holds a 'Mayor of {city}' row."""
    try:
        r = (
            supabase.table("elected_officials")
            .select("id")
            .eq("level", "local")
            .eq("state", "FL")
            .eq("category", "City Government")
            .eq("district", city_name)
            .eq("title", f"Mayor of {city_name}")
            .limit(1)
            .execute()
        )
        return bool(r.data)
    except Exception:
        return False


def insert_officials(
    supabase, muni: dict, result: CityResult, zip_codes: str | None
) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    city = muni["name"]
    mayor_seated = _db_has_mayor(supabase, city)

    for off in result.officials:
        # Hard rule: at most one "Mayor of {city}" in the DB.
        if off.role == "Mayor":
            if mayor_seated:
                log_failure(
                    supabase,
                    muni,
                    f"AMBIGUOUS_MAYOR: extra mayor candidate '{off.name}' skipped "
                    f"(city already has a seated mayor)",
                    off.source_url or result.verified_url,
                )
                skipped += 1
                continue
            mayor_seated = True  # reserve the slot for this insert

        if existing_official_match(supabase, off.name, zip_codes):
            skipped += 1
            continue
        row = {
            "name": off.name,
            "title": title_for(city, off.role),
            "level": "local",
            "branch": branch_for(off.role),
            "state": "FL",
            "category": "City Government",
            "district": city,
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
            LOG.warning("insert failed %s / %s: %s", city, off.name, e)

    # Log rejected mayor candidates surfaced elsewhere on the site.
    for rej in result.rejected_mayors:
        log_failure(
            supabase,
            muni,
            f"AMBIGUOUS_MAYOR: duplicate mayor candidate '{rej.name}' dropped",
            rej.source_url or result.verified_url,
        )

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

    # Enforce at-most-one in-memory mayor before any DB writes.
    result.finalize_mayor()

    # City-level email dedupe: if the same email is attached to ≥2 officials
    # across the full result (not just one page), it's almost certainly a
    # generic city-hall mailbox that got broadcast. Keep it on the mayor only.
    email_counts: dict[str, int] = {}
    for off in result.officials:
        if off.email:
            key = off.email.lower()
            email_counts[key] = email_counts.get(key, 0) + 1
    for shared_email, cnt in email_counts.items():
        if cnt < 2:
            continue
        mayor = next(
            (o for o in result.officials if o.role == "Mayor" and o.email and o.email.lower() == shared_email),
            None,
        )
        for o in result.officials:
            if o.email and o.email.lower() == shared_email and o is not mayor:
                o.email = None

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
