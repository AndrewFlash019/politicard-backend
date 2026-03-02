"""
PolitiScore — Florida State Legislators Ingestion
==================================================
Fetches ALL 160 Florida state legislators (40 senators + 120 reps)
from OpenStates in one pass, then maps each to counties via their
district number.

Run:
  python ingest_fl_legislators.py --dry-run
  python ingest_fl_legislators.py
"""

import os
import time
import argparse
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
OPENSTATES_API_KEY = os.getenv("OPENSTATES_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

ALLOWED_COLUMNS = {"name", "title", "level", "party", "state", "district",
                   "zip_codes", "email", "phone", "website", "photo_url", "branch"}

# ── FL Senate District → Counties mapping ─────────────────────────────────────
# Source: Florida Senate district maps (public record)
FL_SENATE_DISTRICTS = {
    "1":  ["Escambia", "Santa Rosa"],
    "2":  ["Okaloosa", "Walton"],
    "3":  ["Bay", "Calhoun", "Gulf", "Holmes", "Jackson", "Washington"],
    "4":  ["Gadsden", "Jefferson", "Leon", "Liberty", "Madison", "Taylor", "Wakulla"],
    "5":  ["Columbia", "Dixie", "Gilchrist", "Hamilton", "Lafayette", "Levy", "Suwannee", "Union"],
    "6":  ["Alachua", "Bradford", "Putnam"],
    "7":  ["Flagler", "St. Johns", "Volusia"],
    "8":  ["Baker", "Clay", "Nassau"],
    "9":  ["Duval"],
    "10": ["Duval"],
    "11": ["Duval"],
    "12": ["Citrus", "Hernando", "Marion", "Sumter"],
    "13": ["Hillsborough"],
    "14": ["Hillsborough"],
    "15": ["Hillsborough"],
    "16": ["Hillsborough", "Pasco"],
    "17": ["Pasco", "Pinellas"],
    "18": ["Pinellas"],
    "19": ["Pinellas"],
    "20": ["Charlotte", "DeSoto", "Hardee", "Highlands", "Manatee", "Sarasota"],
    "21": ["Manatee", "Sarasota"],
    "22": ["Brevard", "Indian River", "Okeechobee", "St. Lucie"],
    "23": ["Brevard", "Orange"],
    "24": ["Lake", "Orange", "Osceola", "Seminole"],
    "25": ["Orange", "Osceola"],
    "26": ["Orange", "Seminole"],
    "27": ["Orange"],
    "28": ["Collier", "Glades", "Hendry", "Lee", "Monroe"],
    "29": ["Lee"],
    "30": ["Palm Beach"],
    "31": ["Palm Beach"],
    "32": ["Palm Beach"],
    "33": ["Broward"],
    "34": ["Broward"],
    "35": ["Broward"],
    "36": ["Broward", "Miami-Dade"],
    "37": ["Miami-Dade"],
    "38": ["Miami-Dade"],
    "39": ["Miami-Dade"],
    "40": ["Miami-Dade"],
}

# ── FL House District → Counties mapping ──────────────────────────────────────
FL_HOUSE_DISTRICTS = {
    "1":  ["Escambia"],
    "2":  ["Escambia", "Santa Rosa"],
    "3":  ["Santa Rosa"],
    "4":  ["Okaloosa"],
    "5":  ["Okaloosa", "Walton"],
    "6":  ["Bay"],
    "7":  ["Bay", "Calhoun", "Gulf", "Holmes", "Jackson", "Washington"],
    "8":  ["Gadsden", "Jefferson", "Leon", "Liberty", "Madison", "Wakulla"],
    "9":  ["Leon"],
    "10": ["Columbia", "Dixie", "Gilchrist", "Hamilton", "Lafayette", "Levy", "Suwannee", "Union"],
    "11": ["Alachua"],
    "12": ["Alachua", "Bradford", "Putnam", "Union"],
    "13": ["Flagler", "St. Johns"],
    "14": ["St. Johns"],
    "15": ["Duval"],
    "16": ["Duval"],
    "17": ["Duval"],
    "18": ["Duval"],
    "19": ["Baker", "Clay", "Nassau"],
    "20": ["Clay"],
    "21": ["Marion"],
    "22": ["Citrus", "Hernando", "Marion"],
    "23": ["Hernando", "Pasco"],
    "24": ["Pasco"],
    "25": ["Pasco", "Pinellas"],
    "26": ["Pinellas"],
    "27": ["Pinellas"],
    "28": ["Pinellas"],
    "29": ["Hillsborough"],
    "30": ["Hillsborough"],
    "31": ["Hillsborough"],
    "32": ["Hillsborough"],
    "33": ["Hillsborough"],
    "34": ["Hillsborough"],
    "35": ["Hillsborough"],
    "36": ["Manatee"],
    "37": ["Manatee", "Sarasota"],
    "38": ["Sarasota"],
    "39": ["Charlotte", "DeSoto", "Hardee", "Highlands", "Sarasota"],
    "40": ["DeSoto", "Glades", "Hendry", "Highlands", "Okeechobee"],
    "41": ["Lake", "Sumter"],
    "42": ["Lake", "Orange"],
    "43": ["Orange", "Seminole"],
    "44": ["Seminole"],
    "45": ["Orange"],
    "46": ["Orange"],
    "47": ["Orange"],
    "48": ["Orange", "Osceola"],
    "49": ["Osceola"],
    "50": ["Osceola", "Polk"],
    "51": ["Polk"],
    "52": ["Polk"],
    "53": ["Polk"],
    "54": ["Polk", "Hillsborough"],
    "55": ["Brevard"],
    "56": ["Brevard"],
    "57": ["Brevard", "Indian River"],
    "58": ["Indian River", "St. Lucie"],
    "59": ["Martin", "Palm Beach", "St. Lucie"],
    "60": ["Palm Beach"],
    "61": ["Palm Beach"],
    "62": ["Palm Beach"],
    "63": ["Palm Beach"],
    "64": ["Palm Beach"],
    "65": ["Palm Beach", "Broward"],
    "66": ["Broward"],
    "67": ["Broward"],
    "68": ["Broward"],
    "69": ["Broward"],
    "70": ["Broward"],
    "71": ["Broward"],
    "72": ["Broward"],
    "73": ["Broward", "Miami-Dade"],
    "74": ["Miami-Dade"],
    "75": ["Miami-Dade"],
    "76": ["Miami-Dade"],
    "77": ["Miami-Dade"],
    "78": ["Miami-Dade"],
    "79": ["Miami-Dade"],
    "80": ["Miami-Dade"],
    "81": ["Miami-Dade"],
    "82": ["Miami-Dade"],
    "83": ["Miami-Dade"],
    "84": ["Miami-Dade"],
    "85": ["Miami-Dade"],
    "86": ["Miami-Dade"],
    "87": ["Miami-Dade"],
    "88": ["Miami-Dade"],
    "89": ["Miami-Dade"],
    "90": ["Miami-Dade"],
    "91": ["Miami-Dade"],
    "92": ["Miami-Dade"],
    "93": ["Collier"],
    "94": ["Collier", "Lee", "Monroe"],
    "95": ["Lee"],
    "96": ["Lee"],
    "97": ["Charlotte", "Lee"],
    "98": ["Volusia"],
    "99": ["Flagler", "Volusia"],
    "100": ["Volusia"],
    "101": ["Taylor", "Dixie", "Levy", "Gilchrist"],
    "102": ["Columbia", "Hamilton", "Madison", "Suwannee"],
    "103": ["Nassau", "Baker"],
    "104": ["Duval"],
    "105": ["Duval"],
    "106": ["Duval"],
    "107": ["Duval"],
    "108": ["Duval"],
    "109": ["Duval"],
    "110": ["Alachua"],
    "111": ["Marion", "Levy"],
    "112": ["Citrus", "Marion"],
    "113": ["Lake"],
    "114": ["Lake", "Sumter"],
    "115": ["Seminole"],
    "116": ["Seminole", "Orange"],
    "117": ["Orange"],
    "118": ["Orange"],
    "119": ["Orange"],
    "120": ["Monroe", "Miami-Dade"],
}

# Full FL county → ZIP codes mapping (same as ingest_florida_officials.py)
FLORIDA_COUNTIES = {
    "Alachua":      ["32601","32603","32605","32606","32608","32609","32611","32612","32653"],
    "Baker":        ["32040","32063","32087"],
    "Bay":          ["32401","32403","32404","32405","32407","32408","32409","32413"],
    "Bradford":     ["32042","32044","32058","32091"],
    "Brevard":      ["32754","32780","32796","32901","32903","32905","32907","32909","32920","32922","32926","32931","32934","32935","32937","32940","32950","32952","32953","32955"],
    "Broward":      ["33004","33009","33019","33020","33021","33023","33024","33025","33026","33027","33028","33060","33062","33063","33064","33065","33066","33068","33069","33071","33073","33301","33304","33305","33306","33308","33309","33310","33311","33312","33313","33314","33315","33316","33317","33318","33319","33320","33321","33322","33323","33324","33325","33326","33327","33328","33330","33331","33334","33351","33441","33442"],
    "Calhoun":      ["32421","32424","32430","32449"],
    "Charlotte":    ["33946","33947","33948","33950","33952","33953","33954","33955","33980","33981","33983"],
    "Citrus":       ["34428","34429","34431","34432","34433","34434","34436","34442","34446","34448","34450","34452","34453","34461","34465"],
    "Clay":         ["32003","32006","32043","32065","32067","32068","32073"],
    "Collier":      ["34101","34102","34103","34104","34105","34108","34109","34110","34112","34113","34114","34116","34117","34119","34120","34134","34135","34138","34139","34140","34141","34142","34145"],
    "Columbia":     ["32024","32025","32038","32055","32056","32061"],
    "DeSoto":       ["34266","34267","34268","34269"],
    "Dixie":        ["32628","32648","32680"],
    "Duval":        ["32202","32204","32205","32206","32207","32208","32209","32210","32211","32212","32214","32216","32217","32218","32219","32220","32221","32222","32223","32224","32225","32226","32227","32228","32233","32234","32244","32246","32250","32254","32256","32257","32258","32259","32266","32277"],
    "Escambia":     ["32501","32502","32503","32504","32505","32506","32507","32508","32514","32526","32534","32535"],
    "Flagler":      ["32110","32136","32137","32164"],
    "Franklin":     ["32320","32322","32323","32328","32346"],
    "Gadsden":      ["32317","32324","32332","32333","32340","32351","32352","32353"],
    "Gilchrist":    ["32619","32643","32693"],
    "Glades":       ["33430","33440","33471"],
    "Gulf":         ["32456"],
    "Hamilton":     ["32052","32053","32096"],
    "Hardee":       ["33834","33835","33873","33874"],
    "Hendry":       ["33440","33471","33935"],
    "Hernando":     ["34601","34602","34604","34606","34607","34608","34609","34610","34613","34614"],
    "Highlands":    ["33825","33852","33857","33870","33872","33875","33876"],
    "Hillsborough": ["33510","33511","33527","33534","33547","33549","33556","33559","33563","33565","33566","33567","33569","33578","33579","33584","33592","33594","33601","33602","33603","33604","33605","33606","33607","33608","33609","33610","33611","33612","33613","33614","33615","33616","33617","33618","33619","33620","33621","33624","33625","33626","33629","33634","33635","33637","33647"],
    "Holmes":       ["32425","32426","32428","32464"],
    "Indian River": ["32948","32958","32960","32962","32963","32966","32967","32968"],
    "Jackson":      ["32420","32423","32426","32431","32432","32440","32442","32443","32444","32445","32446","32447","32448","32460"],
    "Jefferson":    ["32336","32344"],
    "Lafayette":    ["32066"],
    "Lake":         ["32702","32726","32735","32757","32776","32778","32784","34711","34712","34714","34729","34731","34736","34737","34748","34753","34756","34762","34788","34797"],
    "Lee":          ["33901","33903","33904","33905","33907","33908","33909","33912","33913","33914","33916","33917","33919","33920","33922","33928","33931","33936","33956","33965","33966","33967","33971","33972","33973","33990","33991","33993"],
    "Leon":         ["32301","32303","32304","32305","32306","32308","32309","32310","32311","32312","32317","32318"],
    "Levy":         ["32621","32625","32626","32639","32668","32696"],
    "Liberty":      ["32321","32334"],
    "Madison":      ["32059","32060","32061","32340"],
    "Manatee":      ["34201","34202","34203","34205","34207","34208","34209","34210","34211","34212","34215","34217","34219","34221","34222","34228","34229"],
    "Marion":       ["32113","32134","32179","32617","32667","32686","34420","34421","34470","34471","34472","34473","34474","34475","34476","34479","34480","34481","34482","34488","34491"],
    "Martin":       ["33455","34956","34957","34990","34994","34996","34997"],
    "Miami-Dade":   ["33010","33012","33013","33014","33015","33016","33018","33030","33031","33032","33033","33034","33035","33039","33054","33055","33056","33101","33109","33125","33126","33127","33128","33129","33130","33131","33132","33133","33134","33135","33136","33137","33138","33139","33140","33141","33142","33143","33144","33145","33146","33147","33149","33150","33154","33155","33156","33157","33158","33160","33161","33162","33165","33166","33167","33168","33169","33170","33172","33173","33174","33175","33176","33177","33178","33179","33180","33183","33184","33185","33186","33187","33189","33190","33193","33194","33196"],
    "Monroe":       ["33001","33036","33037","33040","33042","33043","33050","33051","33070"],
    "Nassau":       ["32009","32011","32034","32035","32046","32097"],
    "Okaloosa":     ["32531","32533","32536","32537","32541","32542","32544","32547","32548","32564","32567","32578","32579","32580"],
    "Okeechobee":   ["34972","34973","34974"],
    "Orange":       ["32703","32712","32751","32789","32792","32801","32803","32804","32805","32806","32807","32808","32809","32810","32811","32812","32816","32817","32818","32819","32820","32821","32822","32824","32825","32826","32827","32828","32829","32831","32832","32833","32835","32836","32837","32839","32867","32868","32869","34734","34760","34761","34777","34786","34787"],
    "Osceola":      ["34739","34741","34743","34744","34745","34746","34747","34758","34759","34769","34771","34772","34773"],
    "Palm Beach":   ["33401","33403","33404","33405","33406","33407","33408","33409","33410","33411","33412","33413","33414","33418","33426","33428","33430","33431","33432","33433","33434","33435","33436","33437","33444","33445","33446","33449","33458","33460","33461","33462","33463","33467","33469","33470","33472","33477","33478","33480","33483","33484","33486","33487","33496","33498"],
    "Pasco":        ["33523","33525","33541","33542","33543","33544","33545","33549","33556","33558","33559","33574","33576","34637","34638","34639","34652","34653","34654","34655","34667","34668","34669","34679","34680","34681","34682","34683","34684","34690","34691"],
    "Pinellas":     ["33701","33702","33703","33704","33705","33706","33707","33708","33709","33710","33711","33712","33713","33714","33715","33716","33729","33755","33756","33759","33760","33761","33762","33763","33764","33765","33767","33770","33771","33772","33773","33774","33775","33776","33777","33778","33781","33782","33785","33786"],
    "Polk":         ["33801","33803","33805","33809","33810","33811","33812","33813","33815","33823","33825","33830","33834","33836","33837","33838","33839","33840","33841","33843","33844","33849","33850","33853","33857","33859","33860","33863","33867","33868","33877","33880","33881","33884","33888"],
    "Putnam":       ["32112","32131","32139","32148","32177","32181","32187","32189","32193"],
    "Santa Rosa":   ["32531","32561","32563","32564","32565","32566","32568","32570","32571","32572","32577","32583"],
    "Sarasota":     ["34228","34229","34231","34232","34233","34234","34235","34236","34237","34238","34239","34240","34241","34242","34275","34285","34286","34287","34288","34289","34291","34292","34293"],
    "Seminole":     ["32701","32703","32707","32708","32714","32730","32732","32746","32750","32751","32762","32771","32773","32779"],
    "St. Johns":    ["32033","32080","32081","32082","32084","32086","32092","32095","32259"],
    "St. Lucie":    ["34945","34946","34947","34950","34952","34953","34981","34982","34983","34984","34986","34987","34988"],
    "Sumter":       ["33513","33514","33538","33585","34484","34785"],
    "Suwannee":     ["32008","32060","32064","32071","32094"],
    "Taylor":       ["32347","32348","32356","32359"],
    "Union":        ["32054","32083"],
    "Volusia":      ["32101","32114","32117","32118","32119","32127","32128","32129","32130","32141","32168","32169","32174","32176","32180","32190"],
    "Wakulla":      ["32327","32346"],
    "Walton":       ["32433","32435","32439","32459","32461","32462"],
    "Washington":   ["32427","32428","32437","32438","32462","32466"],
}


def get_zip_codes_for_counties(counties: list) -> str:
    """Return comma-separated ZIP codes for a list of counties."""
    zips = []
    for county in counties:
        zips.extend(FLORIDA_COUNTIES.get(county, []))
    return ",".join(list(dict.fromkeys(zips)))  # deduplicate preserving order


def fetch_all_fl_legislators() -> list:
    """Fetch all FL state legislators from OpenStates people endpoint."""
    all_members = []
    for chamber in ["upper", "lower"]:
        page = 1
        while True:
            try:
                r = requests.get(
                    "https://v3.openstates.org/people",
                    params={
                        "jurisdiction": "fl",
                        "org_classification": chamber,
                        "per_page": 50,
                        "page": page,
                    },
                    headers={"X-API-KEY": OPENSTATES_API_KEY},
                    timeout=15,
                )
                data = r.json()
                results = data.get("results", [])
                if not results:
                    break
                all_members.extend([(m, chamber) for m in results])
                # Check if there's another page
                pagination = data.get("pagination", {})
                if page >= pagination.get("max_page", 1):
                    break
                page += 1
                time.sleep(0.3)
            except Exception as e:
                print(f"  Warning: OpenStates error (chamber={chamber}, page={page}): {e}")
                break
    return all_members


def build_official_record(member: dict, chamber: str) -> dict | None:
    """Convert OpenStates member data to a Supabase record."""
    role = member.get("current_role", {})
    if not role:
        return None

    district = str(role.get("district", "")).strip()
    if not district:
        return None

    if chamber == "upper":
        title = f"State Senator, District {district}"
        district_map = FL_SENATE_DISTRICTS
    else:
        title = f"State Representative, District {district}"
        district_map = FL_HOUSE_DISTRICTS

    counties = district_map.get(district, [])
    zip_codes = get_zip_codes_for_counties(counties)

    party_raw = member.get("party", "")
    if "Republican" in party_raw:
        party = "Republican"
    elif "Democrat" in party_raw:
        party = "Democrat"
    else:
        party = party_raw

    return {
        "name": member.get("name", ""),
        "title": title,
        "level": "state",
        "party": party,
        "state": "FL",
        "district": district,
        "branch": "legislative",
        "zip_codes": zip_codes,
        "photo_url": member.get("image"),
        "website": member.get("openstates_url"),
    }


def upsert_record(record: dict) -> bool:
    """Insert or update one official. Returns True if inserted."""
    clean = {k: v for k, v in record.items() if k in ALLOWED_COLUMNS and v is not None}
    try:
        existing = supabase.table("elected_officials") \
            .select("id") \
            .eq("name", clean["name"]) \
            .eq("title", clean["title"]) \
            .execute()
        if existing.data:
            supabase.table("elected_officials") \
                .update({"zip_codes": clean.get("zip_codes", ""), "photo_url": clean.get("photo_url")}) \
                .eq("id", existing.data[0]["id"]) \
                .execute()
            return False
        else:
            supabase.table("elected_officials").insert(clean).execute()
            return True
    except Exception as e:
        print(f"  ERROR inserting {clean.get('name')}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="FL State Legislators Ingestion")
    parser.add_argument("--dry-run", action="store_true", help="Print without inserting")
    args = parser.parse_args()

    print("=" * 60)
    print("PolitiScore — FL State Legislators Ingestion")
    print("=" * 60)

    print("\nFetching all FL legislators from OpenStates...")
    members = fetch_all_fl_legislators()
    print(f"Found {len(members)} total members\n")

    inserted = 0
    skipped = 0
    errors = 0

    for member, chamber in members:
        record = build_official_record(member, chamber)
        if not record:
            skipped += 1
            continue

        counties = FL_SENATE_DISTRICTS.get(record["district"], []) if chamber == "upper" \
                   else FL_HOUSE_DISTRICTS.get(record["district"], [])

        if args.dry_run:
            print(f"  [DRY RUN] {record['name']} — {record['title']} → {', '.join(counties) or 'NO COUNTIES'}")
            inserted += 1
        else:
            ok = upsert_record(record)
            if ok:
                inserted += 1
                print(f"  Inserted: {record['name']} ({record['title']})")
            else:
                skipped += 1

    print("\n" + "=" * 60)
    action = "would be " if args.dry_run else ""
    print(f"COMPLETE: {inserted} {action}inserted, {skipped} skipped")
    print("=" * 60)


if __name__ == "__main__":
    main()
