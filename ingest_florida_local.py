import requests
import os
import time
import json
from supabase import create_client

# ── CONFIG ────────────────────────────────────────────────────────────────────
CICERO_API_KEY = "519185b4f13cc3df0d69790b4fe75975fb9b7077"
SUPABASE_URL   = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY", "")

# One representative ZIP per Florida county (67 counties)
FL_COUNTY_ZIPS = {
    "Alachua":       "32601",
    "Baker":         "32063",
    "Bay":           "32401",
    "Bradford":      "32091",
    "Brevard":       "32901",
    "Broward":       "33301",
    "Calhoun":       "32421",
    "Charlotte":     "33950",
    "Citrus":        "34428",
    "Clay":          "32043",
    "Collier":       "34102",
    "Columbia":      "32055",
    "DeSoto":        "34266",
    "Dixie":         "32628",
    "Duval":         "32202",
    "Escambia":      "32501",
    "Flagler":       "32137",
    "Franklin":      "32320",
    "Gadsden":       "32301",
    "Gilchrist":     "32693",
    "Glades":        "33471",
    "Gulf":          "32456",
    "Hamilton":      "32052",
    "Hardee":        "33873",
    "Hendry":        "33440",
    "Hernando":      "34601",
    "Highlands":     "33870",
    "Hillsborough":  "33601",
    "Holmes":        "32425",
    "Indian River":  "32960",
    "Jackson":       "32401",
    "Jefferson":     "32344",
    "Lafayette":     "32066",
    "Lake":          "34748",
    "Lee":           "33901",
    "Leon":          "32301",
    "Levy":          "32621",
    "Liberty":       "32321",
    "Madison":       "32340",
    "Manatee":       "34205",
    "Marion":        "34470",
    "Martin":        "34990",
    "Miami-Dade":    "33101",
    "Monroe":        "33040",
    "Nassau":        "32034",
    "Okaloosa":      "32501",
    "Okeechobee":    "34972",
    "Orange":        "32801",
    "Osceola":       "34741",
    "Palm Beach":    "33401",
    "Pasco":         "33525",
    "Pinellas":      "33701",
    "Polk":          "33801",
    "Putnam":        "32177",
    "Saint Johns":   "32084",
    "Saint Lucie":   "34950",
    "Santa Rosa":    "32570",
    "Sarasota":      "34230",
    "Seminole":      "32771",
    "Sumter":        "33585",
    "Suwannee":      "32064",
    "Taylor":        "32347",
    "Union":         "32054",
    "Volusia":       "32114",
    "Wakulla":       "32327",
    "Walton":        "32461",
    "Washington":    "32428",
}

# Local district types we want
LOCAL_DISTRICT_TYPES = ["LOCAL_EXEC", "LOCAL_LOWER", "SCHOOL"]

def fetch_cicero_officials(zip_code, district_type):
    """Fetch officials from Cicero API for a ZIP and district type."""
    url = "https://app.cicerodata.com/v3.1/official"
    params = {
        "key": CICERO_API_KEY,
        "search_loc": zip_code,
        "district_type": district_type,
        "format": "json",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("response", {}).get("errors"):
            print(f"    API error: {data['response']['errors']}")
            return []
        officials = data.get("response", {}).get("results", {}).get("officials", [])
        return officials
    except Exception as e:
        print(f"    Request failed: {e}")
        return []

def map_district_type(dt):
    """Map Cicero district type to our level."""
    if dt in ("LOCAL_EXEC", "LOCAL_LOWER"):
        return "Local"
    if dt == "SCHOOL":
        return "Local"
    return "Local"

def map_title(official, district_type):
    """Build a readable title from Cicero data."""
    office = official.get("office", {})
    title = office.get("title") or official.get("title") or ""
    district = office.get("district", {})
    district_name = district.get("label") or district.get("district_type_label") or ""
    if title and district_name:
        return f"{title}, {district_name}"
    return title or district_name or "Local Official"

def ingest():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    total_inserted = 0
    total_skipped = 0
    credits_used = 0

    for county, zip_code in FL_COUNTY_ZIPS.items():
        print(f"\n📍 {county} County ({zip_code})")
        
        for district_type in LOCAL_DISTRICT_TYPES:
            officials = fetch_cicero_officials(zip_code, district_type)
            credits_used += 1
            time.sleep(0.3)  # Stay well under 200/min rate limit
            
            if not officials:
                print(f"    {district_type}: 0 officials")
                continue

            print(f"    {district_type}: {len(officials)} officials found")

            for o in officials:
                name = f"{o.get('first_name', '')} {o.get('last_name', '')}".strip()
                if not name:
                    continue

                party = o.get("party") or "N/A"
                # Normalize party
                if "Republican" in party or party == "R":
                    party = "R"
                elif "Democrat" in party or party == "D":
                    party = "D"
                elif "Independent" in party or party == "I":
                    party = "I"
                else:
                    party = "N/A"

                title = map_title(o, district_type)
                office = o.get("office", {})
                district = office.get("district", {})

                # Extract state — only keep Florida officials
                st = district.get("state") or o.get("state_name") or ""
                if st and st not in ("FL", "Florida"):
                    continue

                row = {
                    "name":     name,
                    "title":    title,
                    "party":    party,
                    "level":    "Local",
                    "state":    "FL",
                    "county":   county,
                    "district": district.get("label") or "",
                    "email":    o.get("email") or "",
                    "phone":    o.get("phone") or "",
                    "website":  o.get("url") or "",
                    "photo_url":o.get("photo_url") or "",
                    "bio":      "",
                    "source":   "cicero",
                }

                # Upsert by name + county to avoid duplicates
                try:
                    existing = supabase.table("officials") \
                        .select("id") \
                        .eq("name", name) \
                        .eq("county", county) \
                        .eq("level", "Local") \
                        .execute()

                    if existing.data:
                        total_skipped += 1
                    else:
                        supabase.table("officials").insert(row).execute()
                        total_inserted += 1
                        print(f"      ✅ {name} — {title}")
                except Exception as e:
                    print(f"      ❌ Failed to insert {name}: {e}")

    print(f"\n{'='*50}")
    print(f"✅ Done! Inserted: {total_inserted} | Skipped (already exist): {total_skipped}")
    print(f"💳 Estimated credits used: {credits_used}")

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Set SUPABASE_URL and SUPABASE_KEY environment variables first.")
    else:
        ingest()
