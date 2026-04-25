"""Rebuild zip_codes for FL State House + Senate members from Census crosswalks.

Why: many state legislative reps had stale / overlapping zip_codes arrays — e.g.
Palm Coast 32164 was showing three FL House districts at once. The Census Bureau
publishes authoritative ZCTA-to-SLDL (State House, lower) and ZCTA-to-SLDU
(State Senate, upper) crosswalks keyed to the 2024 district boundaries.

Sources (Florida only, state FIPS 12):
  SLDL 2024: https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/tab20_sldl202420_zcta520_st12.txt
  SLDU 2024: https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/tab20_sldu202420_zcta520_st12.txt

Attribution rule: when a ZCTA spans multiple districts, assign it to the district
with the largest land-area overlap (AREALAND_PART) so each ZIP resolves to one rep.

Usage:
    python scripts/fix_fl_house_districts.py            # apply updates
    python scripts/fix_fl_house_districts.py --dry-run  # preview only
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
from collections import defaultdict
from typing import Dict, Set, Tuple

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SLDL_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/"
    "tab20_sldl202420_zcta520_st12.txt"
)
SLDU_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/"
    "tab20_sldu202420_zcta520_st12.txt"
)

log = logging.getLogger("fix_fl_legislature")


def fetch_crosswalk(url: str) -> str:
    log.info("downloading %s", url)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.text


def parse_crosswalk(
    text: str, geoid_col: str
) -> Tuple[Dict[str, int], Dict[int, Set[str]]]:
    """Parse a Census ZCTA-to-SLDL/SLDU crosswalk. GEOID = state FIPS (2) + district (3)."""
    lines = text.splitlines()
    header = lines[0].lstrip("\ufeff").split("|")
    idx = {n: i for i, n in enumerate(header)}
    gcol = idx[geoid_col]
    zcol = idx["GEOID_ZCTA5_20"]
    acol = idx["AREALAND_PART"]

    raw: Dict[str, list] = defaultdict(list)
    for line in lines[1:]:
        parts = line.split("|")
        zcta = parts[zcol].strip()
        if not zcta:
            continue
        g = parts[gcol].strip()
        if len(g) < 3:
            continue
        district = int(g[2:])
        if district in (998, 999):
            continue  # non-district placeholder (e.g. at-large)
        try:
            area = int(parts[acol])
        except ValueError:
            area = 0
        raw[zcta].append((district, area))

    zip_to_dist: Dict[str, int] = {}
    dist_to_zips: Dict[int, Set[str]] = defaultdict(set)
    for zcta, rows in raw.items():
        rows.sort(key=lambda x: x[1], reverse=True)
        best = rows[0][0]
        zip_to_dist[zcta] = best
        dist_to_zips[best].add(zcta)
    return zip_to_dist, dist_to_zips


def update_chamber(
    sb,
    dist_to_zips: Dict[int, Set[str]],
    title_patterns: list[str],
    label: str,
    dry_run: bool,
) -> int:
    rows = []
    for pat in title_patterns:
        res = (
            sb.table("elected_officials")
            .select("id,name,title,district,zip_codes")
            .eq("state", "FL")
            .ilike("title", pat)
            .execute()
        )
        rows.extend(res.data)
    # dedupe by id
    seen, deduped = set(), []
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        deduped.append(r)

    updated = 0
    for row in sorted(deduped, key=lambda r: (int(r.get("district") or 0), r["id"])):
        district = row.get("district")
        try:
            dnum = int(district) if district else None
        except (ValueError, TypeError):
            dnum = None
        if dnum is None:
            log.warning("[%s] cannot parse district for id=%s name=%s", label, row["id"], row["name"])
            continue
        zips = sorted(dist_to_zips.get(dnum, set()))
        new_value = ",".join(zips) if zips else None
        old_zips = (row.get("zip_codes") or "").split(",") if row.get("zip_codes") else []
        old_count = len([z for z in old_zips if z])
        log.info(
            "[%s] D%03d  %-30s  old=%3d  new=%3d%s",
            label, dnum, row["name"][:30], old_count, len(zips),
            "  (dry-run)" if dry_run else "",
        )
        if not dry_run:
            sb.table("elected_officials").update({"zip_codes": new_value}).eq("id", row["id"]).execute()
            updated += 1
    return updated


def verify(
    sb,
    zip_to_dist: Dict[str, int],
    title_patterns: list[str],
    label: str,
    sample_size: int = 10,
) -> None:
    all_zips = list(zip_to_dist.keys())
    random.shuffle(all_zips)
    sample = all_zips[:sample_size]
    if "32164" not in sample:
        sample.append("32164")

    for z in sample:
        # PostgREST or_ doesn't play well with patterns containing commas / %.
        # Query each title pattern separately and union the results.
        combined = []
        for pat in title_patterns:
            r = (
                sb.table("elected_officials")
                .select("id,name,district,title,zip_codes")
                .eq("state", "FL")
                .ilike("title", pat)
                .like("zip_codes", f"%{z}%")
                .execute()
            )
            combined.extend(r.data)
        # Exact comma-token match, dedupe by id
        matches, seen = [], set()
        for r in combined:
            if r["id"] in seen:
                continue
            if z in (r.get("zip_codes") or "").split(","):
                matches.append(r)
                seen.add(r["id"])
        expected = zip_to_dist[z]
        ok = len(matches) == 1 and int(matches[0]["district"]) == expected
        log.info(
            "[%s] verify %s -> expect D%d, got %d match%s %s",
            label, z, expected, len(matches),
            "es" if len(matches) != 1 else "",
            "OK" if ok else "BAD",
        )
        if not ok:
            sb.table("scrape_failures").insert({
                "source_table": "elected_officials",
                "identifier": z,
                "reason": f"DISTRICT_MAPPING_ERROR: zip {z} expected {label} D{expected}, got {len(matches)} matches",
            }).execute()


HOUSE_TITLES = ["Representative, District %", "State Representative, District %"]
SENATE_TITLES = ["Senator, District %", "State Senator, District %"]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--chamber", choices=["house", "senate", "both"], default="both")
    args = parser.parse_args()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
    if not (url and key):
        log.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        return 1
    sb = create_client(url, key)

    if args.chamber in ("house", "both"):
        text = fetch_crosswalk(SLDL_URL)
        z2d, d2z = parse_crosswalk(text, "GEOID_SLDL2024_20")
        log.info("SLDL: %d ZCTAs across %d districts", len(z2d), len(d2z))
        n = update_chamber(sb, d2z, HOUSE_TITLES, "HOUSE", args.dry_run)
        log.info("updated %d FL State House reps (dry_run=%s)", n, args.dry_run)
        if not args.dry_run:
            verify(sb, z2d, HOUSE_TITLES, "HOUSE")
        log.info("Palm Coast 32164 -> FL House D%s (expect 19)", z2d.get("32164"))

    if args.chamber in ("senate", "both"):
        text = fetch_crosswalk(SLDU_URL)
        z2d, d2z = parse_crosswalk(text, "GEOID_SLDU2024_20")
        log.info("SLDU: %d ZCTAs across %d districts", len(z2d), len(d2z))
        n = update_chamber(sb, d2z, SENATE_TITLES, "SENATE", args.dry_run)
        log.info("updated %d FL State Senators (dry_run=%s)", n, args.dry_run)
        if not args.dry_run:
            verify(sb, z2d, SENATE_TITLES, "SENATE")
        log.info("Palm Coast 32164 -> FL Senate D%s (expect 7)", z2d.get("32164"))

    return 0


if __name__ == "__main__":
    sys.exit(main())
