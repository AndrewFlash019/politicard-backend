"""Rebuild zip_codes for FL U.S. House members from the Census ZCTA-to-CD119 crosswalk.

Why: our elected_officials.zip_codes arrays for FL congressional reps drifted from
reality — e.g. Palm Coast 32164 ended up associated with multiple districts. The
Census Bureau publishes the authoritative ZCTA-to-Congressional-District crosswalk
for the 119th Congress (current). Rebuilding from that source guarantees each FL
ZIP maps to exactly one US House district.

Source: https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/tab20_cd11920_zcta520_st12.txt

Attribution rule: when a ZCTA spans multiple districts, assign it to the district
with the largest land-area overlap (AREALAND_PART). This collapses the crosswalk's
many-to-many relationship into a clean one-ZIP-to-one-district map.

Usage:
    python scripts/fix_us_house_districts.py            # apply updates
    python scripts/fix_us_house_districts.py --dry-run  # preview only
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

CROSSWALK_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/"
    "tab20_cd11920_zcta520_st12.txt"
)

log = logging.getLogger("fix_us_house")


def fetch_crosswalk(url: str) -> str:
    log.info("downloading %s", url)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.text


def parse_zip_to_district(text: str) -> Tuple[Dict[str, int], Dict[int, Set[str]]]:
    """Return (zip -> district) and (district -> set of zips).

    Each ZCTA is attributed to the single district with the largest AREALAND_PART.
    """
    lines = text.splitlines()
    header = lines[0].lstrip("\ufeff").split("|")
    idx = {name: i for i, name in enumerate(header)}
    cd_geoid_col = idx["GEOID_CD119_20"]
    zcta_col = idx["GEOID_ZCTA5_20"]
    area_col = idx["AREALAND_PART"]

    raw: Dict[str, list] = defaultdict(list)
    for line in lines[1:]:
        parts = line.split("|")
        zcta = parts[zcta_col].strip()
        if not zcta:
            continue
        cd_geoid = parts[cd_geoid_col].strip()
        if len(cd_geoid) < 3:
            continue
        # GEOID = state FIPS (2) + CD (2). "1206" -> district 6.
        district = int(cd_geoid[2:])
        if district in (98, 99):
            continue
        try:
            area = int(parts[area_col])
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


def update_officials(sb, dist_to_zips: Dict[int, Set[str]], dry_run: bool):
    res = (
        sb.table("elected_officials")
        .select("id,name,title,district,zip_codes")
        .eq("state", "FL")
        .ilike("title", "U.S. Representative%")
        .order("id")
        .execute()
    )
    updated = 0
    for row in res.data:
        district = row.get("district")
        try:
            dnum = int(district) if district else None
        except (ValueError, TypeError):
            dnum = None
        if dnum is None:
            log.warning("cannot parse district for id=%s name=%s", row["id"], row["name"])
            continue
        zips = sorted(dist_to_zips.get(dnum, set()))
        new_value = ",".join(zips) if zips else None
        old_zips = (row.get("zip_codes") or "").split(",") if row.get("zip_codes") else []
        old_count = len([z for z in old_zips if z])
        log.info(
            "FL-%02d  %-30s  old=%3d  new=%3d%s",
            dnum, row["name"][:30], old_count, len(zips),
            "  (dry-run)" if dry_run else "",
        )
        if not dry_run:
            sb.table("elected_officials").update({"zip_codes": new_value}).eq("id", row["id"]).execute()
            updated += 1
    return updated


def verify(sb, zip_to_dist: Dict[str, int], sample_size: int = 10) -> None:
    all_zips = list(zip_to_dist.keys())
    random.shuffle(all_zips)
    sample = all_zips[:sample_size]
    # Include 32164 deterministically
    if "32164" not in sample:
        sample.append("32164")

    for z in sample:
        res = (
            sb.table("elected_officials")
            .select("id,name,district,title")
            .eq("state", "FL")
            .ilike("title", "U.S. Representative%")
            .like("zip_codes", f"%{z}%")
            .execute()
        )
        # filter to exact CSV token match (avoid 32164 matching 321641 substring)
        matches = []
        for r in res.data:
            zips = (r.get("zip_codes") or "").split(",")
            if z in zips:
                matches.append(r)
        expected = zip_to_dist[z]
        ok = len(matches) == 1 and int(matches[0]["district"]) == expected
        log.info(
            "verify %s -> expect FL-%d, got %d match%s %s",
            z, expected, len(matches),
            "es" if len(matches) != 1 else "",
            "OK" if ok else "BAD",
        )
        if not ok:
            sb.table("scrape_failures").insert({
                "source_table": "elected_officials",
                "identifier": z,
                "reason": f"DISTRICT_MAPPING_ERROR: zip {z} expected FL-{expected}, got {len(matches)} matches (US House)",
            }).execute()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
    if not (url and key):
        log.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        return 1
    sb = create_client(url, key)

    text = fetch_crosswalk(CROSSWALK_URL)
    zip_to_dist, dist_to_zips = parse_zip_to_district(text)
    log.info("parsed %d FL ZCTAs across %d US House districts", len(zip_to_dist), len(dist_to_zips))

    updated = update_officials(sb, dist_to_zips, args.dry_run)
    log.info("updated %d FL US Reps (dry_run=%s)", updated, args.dry_run)

    if not args.dry_run:
        verify(sb, zip_to_dist)
    log.info("Palm Coast 32164 -> FL-%s", zip_to_dist.get("32164"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
