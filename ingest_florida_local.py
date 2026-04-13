import requests
import os
import time
from supabase import create_client

CICERO_API_KEY = "519185b4f13cc3df0d69790b4fe75975fb9b7077"
SUPABASE_URL   = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY", "")

FL_COUNTIES = {
    "Alachua":       (29.6516, -82.3248, "Gainesville"),
    "Baker":         (30.2619, -82.1226, "Macclenny"),
    "Bay":           (30.1588, -85.6602, "Panama City"),
    "Bradford":      (29.9441, -82.1310, "Starke"),
    "Brevard":       (28.6122, -80.8076, "Titusville"),
    "Broward":       (26.1224, -80.1373, "Fort Lauderdale"),
    "Calhoun":       (30.4391, -85.0527, "Blountstown"),
    "Charlotte":     (26.9281, -82.0454, "Punta Gorda"),
    "Citrus":        (28.8350, -82.3307, "Inverness"),
    "Clay":          (29.9919, -81.6784, "Green Cove Springs"),
    "Collier":       (26.1420, -81.7948, "Naples"),
    "Columbia":      (30.1896, -82.6390, "Lake City"),
    "DeSoto":        (27.2150, -81.8579, "Arcadia"),
    "Dixie":         (29.6358, -83.1274, "Cross City"),
    "Duval":         (30.3322, -81.6557, "Jacksonville"),
    "Escambia":      (30.4213, -87.2169, "Pensacola"),
    "Flagler":       (29.4650, -81.2558, "Bunnell"),
    "Franklin":      (29.7258, -84.9902, "Apalachicola"),
    "Gadsden":       (30.5835, -84.5866, "Quincy"),
    "Gilchrist":     (29.7891, -82.8130, "Trenton"),
    "Glades":        (26.8312, -81.0854, "Moore Haven"),
    "Gulf":          (29.8158, -85.3052, "Port St Joe"),
    "Hamilton":      (30.4858, -82.9513, "Jasper"),
    "Hardee":        (27.5464, -81.8134, "Wauchula"),
    "Hendry":        (26.7659, -81.0659, "LaBelle"),
    "Hernando":      (28.5436, -82.3790, "Brooksville"),
    "Highlands":     (27.4958, -81.4410, "Sebring"),
    "Hillsborough":  (27.9506, -82.4572, "Tampa"),
    "Holmes":        (30.7891, -85.6686, "Bonifay"),
    "Indian River":  (27.6386, -80.3973, "Vero Beach"),
    "Jackson":       (30.7741, -85.2293, "Marianna"),
    "Jefferson":     (30.5449, -83.8710, "Monticello"),
    "Lafayette":     (30.0558, -83.1741, "Mayo"),
    "Lake":          (28.8014, -81.7229, "Tavares"),
    "Lee":           (26.6406, -81.8723, "Fort Myers"),
    "Leon":          (30.4382, -84.2807, "Tallahassee"),
    "Levy":          (29.4502, -82.6326, "Bronson"),
    "Liberty":       (30.2380, -84.9958, "Bristol"),
    "Madison":       (30.4680, -83.4135, "Madison"),
    "Manatee":       (27.4989, -82.5748, "Bradenton"),
    "Marion":        (29.1872, -82.1401, "Ocala"),
    "Martin":        (27.1975, -80.2528, "Stuart"),
    "Miami-Dade":    (25.7617, -80.1918, "Miami"),
    "Monroe":        (24.5551, -81.7800, "Key West"),
    "Nassau":        (30.6696, -81.4626, "Fernandina Beach"),
    "Okaloosa":      (30.7197, -86.5717, "Crestview"),
    "Okeechobee":    (27.2436, -80.8298, "Okeechobee"),
    "Orange":        (28.5383, -81.3792, "Orlando"),
    "Osceola":       (28.2920, -81.4076, "Kissimmee"),
    "Palm Beach":    (26.7153, -80.0534, "West Palm Beach"),
    "Pasco":         (28.3642, -82.1957, "Dade City"),
    "Pinellas":      (27.9654, -82.8001, "Clearwater"),
    "Polk":          (27.8998, -81.8129, "Bartow"),
    "Putnam":        (29.6480, -81.6376, "Palatka"),
    "Saint Johns":   (29.8943, -81.3145, "St Augustine"),
    "Saint Lucie":   (27.4467, -80.3256, "Fort Pierce"),
    "Santa Rosa":    (30.6327, -87.0497, "Milton"),
    "Sarasota":      (27.3364, -82.5307, "Sarasota"),
    "Seminole":      (28.8116, -81.2681, "Sanford"),
    "Sumter":        (28.6586, -82.0079, "Bushnell"),
    "Suwannee":      (30.2947, -82.9846, "Live Oak"),
    "Taylor":        (30.1144, -83.5835, "Perry"),
    "Union":         (30.0291, -82.3399, "Lake Butler"),
    "Volusia":       (29.0283, -81.3031, "DeLand"),
    "Wakulla":       (30.1724, -84.3747, "Crawfordville"),
    "Walton":        (30.7213, -86.1077, "DeFuniak Springs"),
    "Washington":    (30.7802, -85.5133, "Chipley"),
}

LOCAL_TYPES = {"LOCAL_EXEC", "LOCAL_LOWER", "LOCAL", "SCHOOL"}

def fetch_all_officials(lat, lon):
    url = "https://app.cicerodata.com/v3.1/official"
    all_officials = []
    offset = 0
    while True:
        params = {
            "key": CICERO_API_KEY,
            "lat": lat,
            "lon": lon,
            "format": "json",
            "max": 200,
            "offset": offset,
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            if data.get("response", {}).get("errors"):
                break
            results = data.get("response", {}).get("results", {})
            officials = results.get("officials", [])
            all_officials.extend(officials)
            count = results.get("count", {})
            if count.get("to", 0) >= count.get("total", 0):
                break
            offset += 200
        except Exception as e:
            print(f"    Request failed: {e}")
            break
    return all_officials

def map_title(official):
    office = official.get("office", {})
    title = office.get("title") or official.get("title") or ""
    district = office.get("district", {})
    district_name = district.get("label") or ""
    if title and district_name:
        return f"{title}, {district_name}"
    return title or district_name or "Local Official"

def ingest():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    total_inserted = 0
    total_skipped = 0

    for county, (lat, lon, city) in FL_COUNTIES.items():
        print(f"\n📍 {county} County ({city})")
        all_officials = fetch_all_officials(lat, lon)
        time.sleep(0.4)

        # Filter to local only
        local = [o for o in all_officials
                 if o.get("office", {}).get("district", {}).get("district_type") in LOCAL_TYPES]

        print(f"    Total: {len(all_officials)} | Local: {len(local)}")

        for o in local:
            name = f"{o.get('first_name', '')} {o.get('last_name', '')}".strip()
            if not name:
                continue

            party_raw = o.get("party") or ""
            if "Republican" in party_raw or party_raw == "R":
                party = "R"
            elif "Democrat" in party_raw or party_raw == "D":
                party = "D"
            elif "Independent" in party_raw or party_raw == "I":
                party = "I"
            else:
                party = "N/A"

            title = map_title(o)
            office = o.get("office", {})
            district = office.get("district", {})

            row = {
                "name":      name,
                "title":     title,
                "party":     party,
                "level":     "Local",
                "state":     "FL",
                "district":  district.get("label") or county,
                "zip_codes": "",
                "email":     (o.get("email_addresses") or [""])[0],
                "phone":     "",
                "website":   (o.get("urls") or [""])[0],
                "photo_url": o.get("photo_origin_url") or "",
                "branch":    "executive" if "EXEC" in (district.get("district_type") or "") else "legislative",
            }

            try:
                existing = supabase.table("elected_officials") \
                    .select("id") \
                    .eq("name", name) \
                    .eq("level", "Local") \
                    .eq("state", "FL") \
                    .execute()

                if existing.data:
                    total_skipped += 1
                else:
                    supabase.table("elected_officials").insert(row).execute()
                    total_inserted += 1
                    print(f"      ✅ {name} — {title}")
            except Exception as e:
                print(f"      ❌ Failed to insert {name}: {e}")

    print(f"\n{'='*50}")
    print(f"✅ Done! Inserted: {total_inserted} | Skipped: {total_skipped}")

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Set SUPABASE_URL and SUPABASE_KEY environment variables first.")
    else:
        ingest()
