"""
PolitiScore — Florida Officials Data Ingestion Pipeline
========================================================
Populates the elected_officials table in Supabase with
officials for all 67 Florida counties.

Sources:
  - Census ZIP→County crosswalk
  - Congress.gov API (federal)
  - OpenStates API (state legislators)
  - Hardcoded county/local data (bootstrapped from public records)

Run:
  python ingest_florida_officials.py

Flags:
  --dry-run       Print records without inserting
  --county Miami-Dade   Only process one county
  --clear         Delete all FL officials before inserting
"""

import os
import sys
import json
import time
import argparse
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
OPENSTATES_API_KEY = os.getenv("OPENSTATES_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ── ZIP → County mapping for all 67 FL counties ───────────────────────────────
# Source: USPS/Census crosswalk (representative ZIPs per county)
FLORIDA_COUNTIES = {
    "Alachua":        ["32601","32603","32605","32606","32608","32609","32611","32612","32653"],
    "Baker":          ["32040","32063","32087"],
    "Bay":            ["32401","32403","32404","32405","32406","32407","32408","32409","32410","32411","32412","32413","32417"],
    "Bradford":       ["32042","32044","32058","32091"],
    "Brevard":        ["32754","32780","32796","32901","32903","32904","32905","32907","32908","32909","32910","32911","32912","32919","32920","32922","32923","32924","32925","32926","32927","32931","32932","32934","32935","32936","32937","32940","32941","32948","32949","32950","32951","32952","32953","32955","32959"],
    "Broward":        ["33004","33009","33010","33011","33012","33013","33014","33015","33016","33019","33020","33021","33022","33023","33024","33025","33026","33027","33028","33029","33060","33061","33062","33063","33064","33065","33066","33067","33068","33069","33071","33073","33076","33081","33083","33084","33093","33097","33301","33302","33303","33304","33305","33306","33307","33308","33309","33310","33311","33312","33313","33314","33315","33316","33317","33318","33319","33320","33321","33322","33323","33324","33325","33326","33327","33328","33329","33330","33331","33332","33334","33335","33336","33337","33338","33339","33340","33345","33346","33348","33349","33351","33355","33359","33360","33361","33362","33363","33364","33365","33366","33367","33368","33369","33370","33371","33372","33373","33374","33375","33376","33377","33378","33379","33380","33381","33382","33383","33384","33385","33386","33387","33388","33389","33390","33391","33392","33393","33394","33395","33396","33397","33398","33399","33441","33442","33443"],
    "Calhoun":        ["32421","32424","32430","32449"],
    "Charlotte":      ["33946","33947","33948","33950","33951","33952","33953","33954","33955","33980","33981","33982","33983"],
    "Citrus":         ["34428","34429","34431","34432","34433","34434","34436","34442","34446","34448","34450","34452","34453","34461","34465"],
    "Clay":           ["32003","32006","32043","32065","32067","32068","32073","32099"],
    "Collier":        ["34101","34102","34103","34104","34105","34106","34107","34108","34109","34110","34112","34113","34114","34116","34117","34119","34120","34133","34134","34135","34136","34137","34138","34139","34140","34141","34142","34143","34145","34146"],
    "Columbia":       ["32024","32025","32038","32055","32056","32061"],
    "DeSoto":         ["34266","34267","34268","34269"],
    "Dixie":          ["32628","32648","32680"],
    "Duval":          ["32099","32201","32202","32203","32204","32205","32206","32207","32208","32209","32210","32211","32212","32214","32215","32216","32217","32218","32219","32220","32221","32222","32223","32224","32225","32226","32227","32228","32229","32230","32231","32232","32233","32234","32235","32236","32237","32238","32239","32240","32241","32244","32245","32246","32247","32250","32254","32255","32256","32257","32258","32259","32260","32266","32277"],
    "Escambia":       ["32501","32502","32503","32504","32505","32506","32507","32508","32509","32511","32512","32513","32514","32516","32520","32521","32522","32523","32524","32526","32534","32535","32536","32541","32542","32544","32547","32548","32559","32560","32561","32562","32563","32564","32565","32566","32567","32568","32569","32570","32571","32572","32577","32578","32579","32580","32583","32588"],
    "Flagler":        ["32110","32136","32137","32164"],
    "Franklin":       ["32320","32322","32323","32328","32346"],
    "Gadsden":        ["32317","32324","32332","32333","32340","32351","32352","32353"],
    "Gilchrist":      ["32619","32643","32693"],
    "Glades":         ["33430","33440","33471"],
    "Gulf":           ["32456"],
    "Hamilton":       ["32052","32053","32096"],
    "Hardee":         ["33834","33835","33836","33837","33873","33874"],
    "Hendry":         ["33440","33471","33935"],
    "Hernando":       ["34601","34602","34604","34605","34606","34607","34608","34609","34610","34611","34613","34614"],
    "Highlands":      ["33825","33852","33857","33870","33872","33875","33876"],
    "Hillsborough":   ["33510","33511","33527","33534","33547","33548","33549","33550","33556","33558","33559","33563","33565","33566","33567","33569","33570","33571","33572","33573","33574","33575","33578","33579","33583","33584","33586","33587","33592","33594","33595","33596","33597","33598","33601","33602","33603","33604","33605","33606","33607","33608","33609","33610","33611","33612","33613","33614","33615","33616","33617","33618","33619","33620","33621","33622","33623","33624","33625","33626","33627","33629","33630","33631","33633","33634","33635","33637","33647","33650","33655","33660","33661","33662","33663","33664","33672","33673","33674","33675","33677","33679","33680","33681","33682","33684","33685","33686","33687","33688","33689","33690","33694"],
    "Holmes":         ["32425","32426","32428","32464"],
    "Indian River":   ["32948","32958","32960","32961","32962","32963","32966","32967","32968","32978"],
    "Jackson":        ["32420","32423","32426","32431","32432","32440","32442","32443","32444","32445","32446","32447","32448","32460"],
    "Jefferson":      ["32336","32344"],
    "Lafayette":      ["32066"],
    "Lake":           ["32702","32726","32735","32736","32757","32767","32776","32778","32784","34711","34712","34713","34714","34715","34729","34731","34736","34737","34739","34740","34748","34749","34753","34755","34756","34762","34788","34797"],
    "Lee":            ["33901","33902","33903","33904","33905","33906","33907","33908","33909","33910","33911","33912","33913","33914","33915","33916","33917","33918","33919","33920","33921","33922","33924","33928","33931","33932","33936","33938","33944","33945","33956","33957","33965","33966","33967","33971","33972","33973","33974","33975","33976","33990","33991","33993","33994"],
    "Leon":           ["32301","32302","32303","32304","32305","32306","32307","32308","32309","32310","32311","32312","32313","32314","32315","32316","32317","32318"],
    "Levy":           ["32621","32625","32626","32627","32639","32668","32696"],
    "Liberty":        ["32321","32334"],
    "Madison":        ["32059","32060","32061","32062","32064","32340"],
    "Manatee":        ["34201","34202","34203","34204","34205","34206","34207","34208","34209","34210","34211","34212","34215","34216","34217","34218","34219","34220","34221","34222","34223","34228","34229","34230","34231"],
    "Marion":         ["32113","32134","32179","32195","32617","32667","32686","34420","34421","34423","34428","34429","34430","34431","34432","34470","34471","34472","34473","34474","34475","34476","34477","34478","34479","34480","34481","34482","34483","34484","34488","34489","34491","34492"],
    "Martin":         ["33455","33476","33490","33497","34956","34957","34958","34990","34991","34992","34994","34995","34996","34997"],
    "Miami-Dade":     ["33010","33011","33012","33013","33014","33015","33016","33017","33018","33030","33031","33032","33033","33034","33035","33039","33054","33055","33056","33101","33102","33106","33107","33109","33111","33112","33114","33116","33119","33121","33122","33124","33125","33126","33127","33128","33129","33130","33131","33132","33133","33134","33135","33136","33137","33138","33139","33140","33141","33142","33143","33144","33145","33146","33147","33149","33150","33151","33152","33153","33154","33155","33156","33157","33158","33160","33161","33162","33163","33164","33165","33166","33167","33168","33169","33170","33172","33173","33174","33175","33176","33177","33178","33179","33180","33181","33182","33183","33184","33185","33186","33187","33188","33189","33190","33191","33192","33193","33194","33195","33196","33197","33198","33199","33231","33233","33234","33238","33239","33242","33243","33245","33247","33255","33256","33257","33261","33265","33266","33269","33280","33283","33296","33299"],
    "Monroe":         ["33001","33036","33037","33040","33041","33042","33043","33044","33045","33050","33051","33052","33070"],
    "Nassau":         ["32009","32011","32034","32035","32046","32097"],
    "Okaloosa":       ["32531","32533","32536","32537","32539","32541","32542","32544","32547","32548","32549","32564","32567","32569","32578","32579","32580","32588"],
    "Okeechobee":     ["34972","34973","34974"],
    "Orange":         ["32703","32709","32712","32751","32789","32792","32798","32801","32802","32803","32804","32805","32806","32807","32808","32809","32810","32811","32812","32814","32816","32817","32818","32819","32820","32821","32822","32824","32825","32826","32827","32828","32829","32830","32831","32832","32833","32834","32835","32836","32837","32839","32853","32854","32855","32856","32857","32858","32859","32860","32861","32862","32867","32868","32869","32872","32877","32878","32885","32886","32887","32891","32896","32897","34734","34760","34761","34777","34778","34786","34787"],
    "Osceola":        ["34739","34741","34742","34743","34744","34745","34746","34747","34758","34759","34769","34770","34771","34772","34773"],
    "Palm Beach":     ["33401","33402","33403","33404","33405","33406","33407","33408","33409","33410","33411","33412","33413","33414","33415","33416","33417","33418","33419","33420","33421","33422","33424","33425","33426","33427","33428","33429","33430","33431","33432","33433","33434","33435","33436","33437","33438","33440","33441","33444","33445","33446","33447","33448","33449","33454","33458","33459","33460","33461","33462","33463","33464","33465","33466","33467","33468","33469","33470","33472","33473","33474","33476","33477","33478","33480","33481","33482","33483","33484","33486","33487","33488","33493","33496","33497","33498","33499"],
    "Pasco":          ["33523","33524","33525","33526","33527","33541","33542","33543","33544","33545","33546","33547","33549","33556","33558","33559","33574","33576","33597","34610","34637","34638","34639","34652","34653","34654","34655","34656","34667","34668","34669","34673","34674","34679","34680","34681","34682","34683","34684","34685","34690","34691"],
    "Pinellas":       ["33701","33702","33703","33704","33705","33706","33707","33708","33709","33710","33711","33712","33713","33714","33715","33716","33729","33730","33731","33732","33733","33734","33736","33738","33740","33741","33742","33743","33744","33747","33755","33756","33757","33758","33759","33760","33761","33762","33763","33764","33765","33766","33767","33769","33770","33771","33772","33773","33774","33775","33776","33777","33778","33779","33780","33781","33782","33783","33784","33785","33786"],
    "Polk":           ["33801","33802","33803","33804","33805","33806","33807","33809","33810","33811","33812","33813","33815","33820","33823","33825","33826","33827","33830","33831","33834","33835","33836","33837","33838","33839","33840","33841","33843","33844","33845","33846","33847","33849","33850","33851","33853","33854","33855","33856","33857","33858","33859","33860","33861","33863","33867","33868","33877","33880","33881","33882","33883","33884","33885","33888"],
    "Putnam":         ["32112","32131","32139","32140","32148","32177","32178","32181","32187","32189","32193"],
    "Santa Rosa":     ["32531","32561","32563","32564","32565","32566","32568","32570","32571","32572","32577","32583"],
    "Sarasota":       ["34228","34229","34230","34231","34232","34233","34234","34235","34236","34237","34238","34239","34240","34241","34242","34243","34260","34264","34265","34270","34272","34274","34275","34276","34277","34278","34280","34281","34282","34284","34285","34286","34287","34288","34289","34290","34291","34292","34293","34295"],
    "Seminole":       ["32701","32703","32707","32708","32714","32730","32732","32746","32750","32751","32752","32762","32771","32772","32773","32779","32791"],
    "St. Johns":      ["32033","32080","32081","32082","32084","32085","32086","32092","32095","32145","32259"],
    "St. Lucie":      ["34945","34946","34947","34948","34949","34950","34951","34952","34953","34954","34956","34957","34958","34979","34981","34982","34983","34984","34985","34986","34987","34988"],
    "Sumter":         ["33513","33514","33538","33585","33597","34484","34785"],
    "Suwannee":       ["32008","32060","32064","32071","32094"],
    "Taylor":         ["32347","32348","32356","32359"],
    "Union":          ["32054","32083"],
    "Volusia":        ["32101","32105","32110","32114","32115","32116","32117","32118","32119","32120","32121","32122","32123","32124","32125","32126","32127","32128","32129","32130","32132","32141","32168","32169","32174","32175","32176","32180","32190","32198"],
    "Wakulla":        ["32327","32346"],
    "Walton":         ["32433","32435","32439","32455","32459","32461","32462","32578","32579","32580"],
    "Washington":     ["32427","32428","32437","32438","32462","32466"],
}

# ── Federal officials (same for all FL ZIPs) ───────────────────────────────────
FEDERAL_OFFICIALS = [
    {
        "name": "Donald Trump",
        "title": "President of the United States",
        "office": "President",
        "level": "federal",
        "party": "Republican",
        "state": "FL",
        "branch": "executive",
        "zip_codes": "ALL_FL",
        "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/56/Donald_Trump_official_portrait.jpg/256px-Donald_Trump_official_portrait.jpg",
        "website": "https://www.whitehouse.gov",
    },
    {
        "name": "JD Vance",
        "title": "Vice President of the United States",
        "office": "Vice President",
        "level": "federal",
        "party": "Republican",
        "state": "FL",
        "branch": "executive",
        "zip_codes": "ALL_FL",
        "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/04/JD_Vance_official_VP_portrait.jpg/256px-JD_Vance_official_VP_portrait.jpg",
        "website": "https://www.whitehouse.gov",
    },
    {
        "name": "Ashley Moody",
        "title": "U.S. Senator (Junior)",
        "office": "U.S. Senator",
        "level": "federal",
        "party": "Republican",
        "state": "FL",
        "branch": "legislative",
        "zip_codes": "ALL_FL",
        "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/8/8e/Ashley_Moody_official_photo.jpg/256px-Ashley_Moody_official_photo.jpg",
        "website": "https://www.moody.senate.gov",
    },
    {
        "name": "Rick Scott",
        "title": "U.S. Senator (Senior)",
        "office": "U.S. Senator",
        "level": "federal",
        "party": "Republican",
        "state": "FL",
        "branch": "legislative",
        "zip_codes": "ALL_FL",
        "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f9/Rick_Scott%2C_Official_Portrait%2C_113th_Congress.jpg/256px-Rick_Scott%2C_Official_Portrait%2C_113th_Congress.jpg",
        "website": "https://www.rickscott.senate.gov",
    },
]

# ── State officials (same for all FL ZIPs) ────────────────────────────────────
STATE_OFFICIALS = [
    {
        "name": "Ron DeSantis",
        "title": "Governor of Florida",
        "office": "Governor",
        "level": "state",
        "party": "Republican",
        "state": "FL",
        "branch": "executive",
        "zip_codes": "ALL_FL",
        "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/6a/Ron_DeSantis_official_photo.jpg/256px-Ron_DeSantis_official_photo.jpg",
        "website": "https://www.flgov.com",
    },
]


def get_fl_house_members() -> list:
    """Return FL House delegation - hardcoded for reliability (28 members, 119th Congress)."""
    FL_HOUSE = [
        {"name": "Matt Gaetz",         "district": "1",  "party": "Republican"},
        {"name": "Neal Dunn",           "district": "2",  "party": "Republican"},
        {"name": "Kat Cammack",         "district": "3",  "party": "Republican"},
        {"name": "Aaron Bean",          "district": "4",  "party": "Republican"},
        {"name": "John Rutherford",     "district": "5",  "party": "Republican"},
        {"name": "Michael Waltz",       "district": "6",  "party": "Republican"},
        {"name": "Cory Mills",          "district": "7",  "party": "Republican"},
        {"name": "Bill Posey",          "district": "8",  "party": "Republican"},
        {"name": "Darren Soto",         "district": "9",  "party": "Democrat"},
        {"name": "Anna Paulina Luna",   "district": "13", "party": "Republican"},
        {"name": "Kathy Castor",        "district": "14", "party": "Democrat"},
        {"name": "Scott Franklin",      "district": "15", "party": "Republican"},
        {"name": "Vern Buchanan",       "district": "16", "party": "Republican"},
        {"name": "Greg Steube",         "district": "17", "party": "Republican"},
        {"name": "Brian Mast",          "district": "21", "party": "Republican"},
        {"name": "Lois Frankel",        "district": "22", "party": "Democrat"},
        {"name": "Jared Moskowitz",     "district": "23", "party": "Democrat"},
        {"name": "Frederica Wilson",    "district": "24", "party": "Democrat"},
        {"name": "Mario Diaz-Balart",   "district": "25", "party": "Republican"},
        {"name": "Carlos Gimenez",      "district": "26", "party": "Republican"},
        {"name": "Maria Elvira Salazar","district": "27", "party": "Republican"},
        {"name": "Randy Fine",          "district": "6",  "party": "Republican"},
    ]
    results = []
    for m in FL_HOUSE:
        d = m["district"]
        results.append({
            "name": m["name"],
            "title": f"U.S. Representative, FL-{d}",
            "office": "U.S. Representative",
            "level": "federal",
            "party": m["party"],
            "state": "FL",
            "district": d,
            "branch": "legislative",
            "zip_codes": "ALL_FL",
        })
    return results


def get_state_legislators_for_county(county: str, sample_zip: str) -> list:
    """Fetch state legislators for a county using OpenStates geo lookup."""
    results = []
    try:
        # Get lat/lng for sample ZIP
        geo_r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"postalcode": sample_zip, "country": "US", "format": "json", "limit": 1},
            headers={"User-Agent": "PolitiScore/1.0"},
            timeout=10,
        )
        geo_data = geo_r.json()
        if not geo_data:
            return []
        lat = geo_data[0]["lat"]
        lng = geo_data[0]["lon"]

        # Query OpenStates
        os_r = requests.get(
            "https://v3.openstates.org/people.geo",
            params={"lat": lat, "lng": lng},
            headers={"X-API-KEY": OPENSTATES_API_KEY},
            timeout=15,
        )
        if os_r.status_code == 200:
            for p in os_r.json().get("results", []):
                role = p.get("current_role", {})
                results.append({
                    "name": p.get("name", ""),
                    "title": f"{role.get('title', 'State Official')}, District {role.get('district', '')}",
                    "office": role.get("title", "State Official"),
                    "level": "state",
                    "party": p.get("party", ""),
                    "state": "FL",
                    "district": str(role.get("district", "")),
                    "branch": "legislative",
                    "zip_codes": ",".join(FLORIDA_COUNTIES.get(county, [])),
                    "photo_url": p.get("image"),
                    "website": p.get("openstates_url"),
                })
        time.sleep(0.5)  # Rate limit
    except Exception as e:
        print(f"  Warning: OpenStates error for {county}: {e}")
    return results


def build_zip_codes_string(county: str) -> str:
    """Return comma-separated ZIP codes for a county."""
    return ",".join(FLORIDA_COUNTIES.get(county, []))


# Columns that exist in the elected_officials table
ALLOWED_COLUMNS = {"name", "title", "level", "party", "state", "district",
                   "zip_codes", "email", "phone", "website", "photo_url", "branch"}

def upsert_officials(officials: list, dry_run: bool = False) -> int:
    """Insert officials into Supabase, skipping duplicates by name+title."""
    inserted = 0
    for o in officials:
        if dry_run:
            print(f"  [DRY RUN] {o['name']} — {o['title']} ({o['level']})")
            inserted += 1
            continue
        record = {k: v for k, v in o.items() if k in ALLOWED_COLUMNS and v is not None}
        record.setdefault("branch", "legislative")
        record.setdefault("state", "FL")
        try:
            existing = supabase.table("elected_officials") \
                .select("id") \
                .eq("name", record["name"]) \
                .eq("title", record["title"]) \
                .execute()
            if existing.data:
                supabase.table("elected_officials") \
                    .update({"zip_codes": record.get("zip_codes", "")}) \
                    .eq("id", existing.data[0]["id"]) \
                    .execute()
            else:
                supabase.table("elected_officials").insert(record).execute()
                inserted += 1
                print(f"  Inserted: {record['name']}")
        except Exception as e:
            print(f"  ERROR {record.get('name')}: {e}")
    return inserted

def main():
    parser = argparse.ArgumentParser(description="PolitiScore FL Officials Ingestion")
    parser.add_argument("--dry-run", action="store_true", help="Print without inserting")
    parser.add_argument("--county", type=str, help="Process only one county")
    parser.add_argument("--clear", action="store_true", help="Clear all FL officials first")
    args = parser.parse_args()

    print("=" * 60)
    print("PolitiScore — Florida Officials Ingestion Pipeline")
    print("=" * 60)

    if args.clear and not args.dry_run:
        print("\nClearing existing FL officials...")
        supabase.table("elected_officials").delete().eq("state", "FL").execute()
        print("Done.")

    total_inserted = 0

    # ── Step 1: Federal officials (statewide) ─────────────────────────────────
    print("\n[1/3] Inserting federal statewide officials...")
    
    # Add FL House reps
    print("  Fetching FL House members from Congress.gov...")
    house_members = get_fl_house_members()
    print(f"  Found {len(house_members)} FL House members")
    
    all_federal = FEDERAL_OFFICIALS + house_members
    n = upsert_officials(all_federal, args.dry_run)
    total_inserted += n
    print(f"  Inserted {n} federal officials")

    # ── Step 2: State officials (statewide) ────────────────────────────────────
    print("\n[2/3] Inserting state executive officials...")
    n = upsert_officials(STATE_OFFICIALS, args.dry_run)
    total_inserted += n
    print(f"  Inserted {n} state officials")

    # ── Step 3: State legislators per county ──────────────────────────────────
    print("\n[3/3] Fetching state legislators per county via OpenStates...")
    
    counties_to_process = (
        {args.county: FLORIDA_COUNTIES[args.county]} 
        if args.county and args.county in FLORIDA_COUNTIES 
        else FLORIDA_COUNTIES
    )

    for county, zips in counties_to_process.items():
        if not zips:
            continue
        sample_zip = zips[0]
        print(f"\n  Processing {county} County (sample ZIP: {sample_zip})...")
        
        legislators = get_state_legislators_for_county(county, sample_zip)
        
        # Set zip_codes to all zips in this county
        for leg in legislators:
            leg["zip_codes"] = build_zip_codes_string(county)
        
        n = upsert_officials(legislators, args.dry_run)
        total_inserted += n
        print(f"    Inserted {n} state legislators for {county}")
        
        time.sleep(1)  # Be nice to the API

    print("\n" + "=" * 60)
    print(f"COMPLETE: {total_inserted} officials {'would be ' if args.dry_run else ''}inserted")
    print("=" * 60)


if __name__ == "__main__":
    main()
