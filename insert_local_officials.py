import os
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

POLK_ZIPS = "33801,33803,33805,33809,33810,33811,33812,33813,33815,33823,33825,33827,33830,33837,33838,33839,33841,33843,33844,33849,33850,33851,33853,33859,33860,33868,33880,33881,33884"
BREVARD_ZIPS = "32901,32903,32904,32905,32907,32908,32909,32920,32922,32925,32926,32927,32931,32934,32935,32937,32940,32948,32949,32950,32951,32952,32953,32955,32958,32959,32976"
VOLUSIA_ZIPS = "32114,32117,32118,32119,32124,32127,32128,32129,32130,32132,32141,32168,32169,32174,32176,32180,32190,32198"
FLAGLER_ZIPS = "32110,32136,32137,32164"

OFFICIALS = [
    # ── HILLSBOROUGH (Tampa) ─────────────────────────────────────────────────
    {"name":"Jane Castor","title":"Mayor, City of Tampa","party":"D","level":"Local","state":"FL","district":"Tampa","branch":"executive","website":"https://www.tampa.gov/mayor","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Alan Clendenin","title":"City Council Chairman, District 1","party":"N/A","level":"Local","state":"FL","district":"Tampa District 1","branch":"legislative","website":"https://www.tampa.gov/city-council","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Guido Maniscalco","title":"City Council Member, District 2","party":"N/A","level":"Local","state":"FL","district":"Tampa District 2","branch":"legislative","website":"https://www.tampa.gov/city-council","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Lynn Hurtak","title":"City Council Member, District 3","party":"N/A","level":"Local","state":"FL","district":"Tampa District 3","branch":"legislative","website":"https://www.tampa.gov/city-council","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Bill Carlson","title":"City Council Member, District 4","party":"N/A","level":"Local","state":"FL","district":"Tampa District 4","branch":"legislative","website":"https://www.tampa.gov/city-council","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Naya Young","title":"City Council Member, District 5","party":"N/A","level":"Local","state":"FL","district":"Tampa District 5","branch":"legislative","website":"https://www.tampa.gov/city-council","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Charlie Miranda","title":"City Council Member, District 6","party":"N/A","level":"Local","state":"FL","district":"Tampa District 6","branch":"legislative","website":"https://www.tampa.gov/city-council","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Luis Viera","title":"City Council Member, District 7","party":"N/A","level":"Local","state":"FL","district":"Tampa District 7","branch":"legislative","website":"https://www.tampa.gov/city-council","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Harry Cohen","title":"County Commissioner, District 1","party":"D","level":"Local","state":"FL","district":"Hillsborough District 1","branch":"legislative","website":"https://www.hillsboroughcounty.org/government/commissioners","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Ken Hagan","title":"County Commissioner, District 2","party":"R","level":"Local","state":"FL","district":"Hillsborough District 2","branch":"legislative","website":"https://www.hillsboroughcounty.org/government/commissioners","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Gwen Myers","title":"County Commissioner, District 3","party":"D","level":"Local","state":"FL","district":"Hillsborough District 3","branch":"legislative","website":"https://www.hillsboroughcounty.org/government/commissioners","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Christine Miller","title":"County Commissioner, District 4","party":"R","level":"Local","state":"FL","district":"Hillsborough District 4","branch":"legislative","website":"https://www.hillsboroughcounty.org/government/commissioners","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Donna Cameron Cepeda","title":"County Commissioner, District 5","party":"R","level":"Local","state":"FL","district":"Hillsborough District 5","branch":"legislative","website":"https://www.hillsboroughcounty.org/government/commissioners","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Chris Boles","title":"County Commissioner, District 6","party":"R","level":"Local","state":"FL","district":"Hillsborough District 6","branch":"legislative","website":"https://www.hillsboroughcounty.org/government/commissioners","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},
    {"name":"Joshua Wostal","title":"County Commissioner, District 7","party":"R","level":"Local","state":"FL","district":"Hillsborough District 7","branch":"legislative","website":"https://www.hillsboroughcounty.org/government/commissioners","zip_codes":"33601,33602,33603,33604,33605,33606,33607,33608,33609,33610,33611,33612,33613,33614,33615,33616,33617,33618,33619,33621,33629,33634,33635,33637,33647"},

    # ── MIAMI-DADE ───────────────────────────────────────────────────────────
    {"name":"Daniella Levine Cava","title":"Mayor, Miami-Dade County","party":"D","level":"Local","state":"FL","district":"Miami-Dade","branch":"executive","website":"https://www.miamidade.gov/global/government/mayor/home.page","zip_codes":"33101,33125,33126,33127,33128,33129,33130,33131,33132,33133,33134,33135,33136,33137,33138,33139,33140,33141,33142,33143,33144,33145,33146,33147,33149,33150,33155,33156,33157,33158,33160,33161,33162,33165,33166,33167,33168,33169,33170"},
    {"name":"Eileen Higgins","title":"Mayor, City of Miami","party":"D","level":"Local","state":"FL","district":"Miami","branch":"executive","website":"https://www.miami.gov","zip_codes":"33101,33125,33126,33127,33128,33129,33130,33131,33132,33133,33134,33135,33136,33137,33138,33139,33140,33141,33142,33143,33144,33145,33146,33147,33149,33150"},
    {"name":"Oliver G. Gilbert III","title":"County Commissioner, District 1","party":"D","level":"Local","state":"FL","district":"Miami-Dade District 1","branch":"legislative","website":"https://www.miamidade.gov/global/government/commission/home.page","zip_codes":"33101,33125,33126,33127,33128,33129,33130,33131,33132,33133,33134,33135,33136,33137,33138,33139,33140,33141,33142,33143,33144,33145,33146,33147,33149,33150"},
    {"name":"Vicki L. Lopez","title":"County Commissioner, District 5","party":"R","level":"Local","state":"FL","district":"Miami-Dade District 5","branch":"legislative","website":"https://www.miamidade.gov/global/government/commission/home.page","zip_codes":"33101,33125,33126,33127,33128,33129,33130,33131,33132,33133,33134,33135,33136,33137,33138,33139,33140,33141,33142,33143,33144,33145,33146,33147,33149,33150"},
    {"name":"Raquel A. Regalado","title":"County Commissioner, District 7","party":"R","level":"Local","state":"FL","district":"Miami-Dade District 7","branch":"legislative","website":"https://www.miamidade.gov/global/government/commission/home.page","zip_codes":"33101,33125,33126,33127,33128,33129,33130,33131,33132,33133,33134,33135,33136,33137,33138,33139,33140,33141,33142,33143,33144,33145,33146,33147,33149,33150"},
    {"name":"Kionne L. McGhee","title":"County Commissioner, District 9","party":"D","level":"Local","state":"FL","district":"Miami-Dade District 9","branch":"legislative","website":"https://www.miamidade.gov/global/government/commission/home.page","zip_codes":"33101,33125,33126,33127,33128,33129,33130,33131,33132,33133,33134,33135,33136,33137,33138,33139,33140,33141,33142,33143,33144,33145,33146,33147,33149,33150"},
    {"name":"Roberto J. Gonzalez","title":"County Commissioner, District 11","party":"R","level":"Local","state":"FL","district":"Miami-Dade District 11","branch":"legislative","website":"https://www.miamidade.gov/global/government/commission/home.page","zip_codes":"33101,33125,33126,33127,33128,33129,33130,33131,33132,33133,33134,33135,33136,33137,33138,33139,33140,33141,33142,33143,33144,33145,33146,33147,33149,33150"},

    # ── ORANGE (Orlando) ─────────────────────────────────────────────────────
    {"name":"Buddy Dyer","title":"Mayor, City of Orlando","party":"D","level":"Local","state":"FL","district":"Orlando","branch":"executive","website":"https://www.orlando.gov/Mayor","zip_codes":"32801,32803,32804,32805,32806,32807,32808,32809,32810,32811,32812,32814,32817,32818,32819,32820,32821,32822,32824,32825,32826,32827,32828,32829,32831,32832,32835,32836,32837,32839"},
    {"name":"Jerry Demings","title":"Mayor, Orange County","party":"D","level":"Local","state":"FL","district":"Orange County","branch":"executive","website":"https://www.orangecountyfl.net/Mayor","zip_codes":"32801,32803,32804,32805,32806,32807,32808,32809,32810,32811,32812,32814,32817,32818,32819,32820,32821,32822,32824,32825,32826,32827,32828,32829,32831,32832,32835,32836,32837,32839"},

    # ── BROWARD ──────────────────────────────────────────────────────────────
    {"name":"Judith Stern","title":"Mayor, Broward County","party":"D","level":"Local","state":"FL","district":"Broward County","branch":"executive","website":"https://www.broward.org/Commission","zip_codes":"33301,33304,33305,33306,33308,33309,33310,33311,33312,33313,33314,33315,33316,33317,33319,33321,33322,33323,33324,33325,33326,33328,33334"},
    {"name":"Dean Trantalis","title":"Mayor, City of Fort Lauderdale","party":"D","level":"Local","state":"FL","district":"Fort Lauderdale","branch":"executive","website":"https://www.fortlauderdale.gov/government/mayor","zip_codes":"33301,33304,33305,33306,33308,33309,33310,33311,33312,33313,33314,33315,33316,33317"},

    # ── PALM BEACH ───────────────────────────────────────────────────────────
    {"name":"Maria Sachs","title":"Mayor, Palm Beach County","party":"D","level":"Local","state":"FL","district":"Palm Beach County","branch":"executive","website":"https://discover.pbcgov.org/bcc","zip_codes":"33401,33403,33404,33405,33406,33407,33408,33409,33410,33411,33412,33413,33414,33415,33417,33418,33426,33428,33431,33432,33433,33434,33435,33436,33437,33444,33445,33446,33458,33460,33461,33462,33463,33467,33480,33483,33484,33486,33487,33496,33498"},
    {"name":"Keith James","title":"Mayor, City of West Palm Beach","party":"D","level":"Local","state":"FL","district":"West Palm Beach","branch":"executive","website":"https://www.wpb.org/government/mayor","zip_codes":"33401,33403,33404,33405,33406,33407,33408,33409,33410,33411"},

    # ── DUVAL (Jacksonville) ─────────────────────────────────────────────────
    {"name":"Donna Deegan","title":"Mayor, City of Jacksonville","party":"D","level":"Local","state":"FL","district":"Jacksonville","branch":"executive","website":"https://www.coj.net/mayor","zip_codes":"32099,32201,32202,32203,32204,32205,32206,32207,32208,32209,32210,32211,32212,32214,32216,32217,32218,32219,32220,32221,32222,32223,32224,32225,32226,32227,32228,32233,32244,32246,32250,32254,32256,32257,32258,32266,32277"},

    # ── PINELLAS ─────────────────────────────────────────────────────────────
    {"name":"Ken Welch","title":"Mayor, City of St. Petersburg","party":"D","level":"Local","state":"FL","district":"St. Petersburg","branch":"executive","website":"https://www.stpete.org/government/mayor","zip_codes":"33701,33702,33703,33704,33705,33706,33707,33708,33709,33710,33711,33712,33713,33714,33715,33716"},
    {"name":"Brian Aungst Jr.","title":"Mayor, City of Clearwater","party":"R","level":"Local","state":"FL","district":"Clearwater","branch":"executive","website":"https://www.myclearwater.com","zip_codes":"33755,33756,33759,33760,33761,33762,33763,33764,33765,33767"},

    # ── POLK ─────────────────────────────────────────────────────────────────
    {"name":"Martha Santiago","title":"County Commissioner, Chair","party":"R","level":"Local","state":"FL","district":"Polk County Chair","branch":"executive","website":"https://www.polkfl.gov/about/board-of-county-commissioners/","zip_codes":POLK_ZIPS},
    {"name":"Becky Troutman","title":"County Commissioner, District 1","party":"R","level":"Local","state":"FL","district":"Polk District 1","branch":"legislative","website":"https://www.polkfl.gov/about/board-of-county-commissioners/district-1/","zip_codes":POLK_ZIPS},
    {"name":"Rick Wilson","title":"County Commissioner, District 2","party":"R","level":"Local","state":"FL","district":"Polk District 2","branch":"legislative","website":"https://www.polkfl.gov/about/board-of-county-commissioners/","zip_codes":POLK_ZIPS},
    {"name":"Bill Braswell","title":"County Commissioner, District 3, Vice Chair","party":"R","level":"Local","state":"FL","district":"Polk District 3","branch":"legislative","website":"https://www.polkfl.gov/about/board-of-county-commissioners/","zip_codes":POLK_ZIPS},
    {"name":"George Lindsey","title":"County Commissioner, District 4","party":"R","level":"Local","state":"FL","district":"Polk District 4","branch":"legislative","website":"https://www.polkfl.gov/about/board-of-county-commissioners/","zip_codes":POLK_ZIPS},
    {"name":"Michael Scott","title":"County Commissioner, District 5","party":"R","level":"Local","state":"FL","district":"Polk District 5","branch":"legislative","website":"https://www.polkfl.gov/about/board-of-county-commissioners/","zip_codes":POLK_ZIPS},

    # ── BREVARD ──────────────────────────────────────────────────────────────
    {"name":"Katie Delaney","title":"County Commissioner, District 1","party":"R","level":"Local","state":"FL","district":"Brevard District 1","branch":"legislative","website":"https://www.brevardfl.gov/CountyCommission/District1/home","zip_codes":BREVARD_ZIPS},
    {"name":"Tom Goodson","title":"County Commissioner, District 2, Vice Chair","party":"R","level":"Local","state":"FL","district":"Brevard District 2","branch":"legislative","website":"https://www.brevardfl.gov/CountyCommission","zip_codes":BREVARD_ZIPS},
    {"name":"Kim Adkinson","title":"County Commissioner, District 3","party":"R","level":"Local","state":"FL","district":"Brevard District 3","branch":"legislative","website":"https://www.brevardfl.gov/CountyCommission/District3/CommissionerStaff","zip_codes":BREVARD_ZIPS},
    {"name":"Rob Feltner","title":"County Commissioner, District 4, Chair","party":"R","level":"Local","state":"FL","district":"Brevard District 4","branch":"executive","website":"https://www.brevardfl.gov/CountyCommission","zip_codes":BREVARD_ZIPS},
    {"name":"Thad Altman","title":"County Commissioner, District 5","party":"R","level":"Local","state":"FL","district":"Brevard District 5","branch":"legislative","website":"https://www.brevardfl.gov/CountyCommission/District5/CommissionerStaff","zip_codes":BREVARD_ZIPS},

    # ── VOLUSIA ──────────────────────────────────────────────────────────────
    {"name":"Jeff Brower","title":"County Chair, Volusia County","party":"R","level":"Local","state":"FL","district":"Volusia County Chair","branch":"executive","website":"https://www.volusia.org/government/county-council/","zip_codes":VOLUSIA_ZIPS,"email":"JBrower@volusia.org"},
    {"name":"Jake Johansson","title":"County Council Member, At-Large","party":"R","level":"Local","state":"FL","district":"Volusia At-Large","branch":"legislative","website":"https://www.volusia.org/government/county-council/","zip_codes":VOLUSIA_ZIPS,"email":"JJohansson@volusia.org"},
    {"name":"Don Dempsey","title":"County Council Member, District 1","party":"R","level":"Local","state":"FL","district":"Volusia District 1","branch":"legislative","website":"https://www.volusia.org/government/county-council/","zip_codes":VOLUSIA_ZIPS,"email":"DDempsey@volusia.org"},
    {"name":"Matt Reinhart","title":"County Council Member, District 2, Vice Chair","party":"R","level":"Local","state":"FL","district":"Volusia District 2","branch":"legislative","website":"https://www.volusia.org/government/county-council/","zip_codes":VOLUSIA_ZIPS,"email":"MReinhart@volusia.org"},
    {"name":"Danny Robins","title":"County Council Member, District 3","party":"R","level":"Local","state":"FL","district":"Volusia District 3","branch":"legislative","website":"https://www.volusia.org/government/county-council/","zip_codes":VOLUSIA_ZIPS,"email":"DRobins@volusia.org"},
    {"name":"Troy Kent","title":"County Council Member, District 4","party":"R","level":"Local","state":"FL","district":"Volusia District 4","branch":"legislative","website":"https://www.volusia.org/government/county-council/","zip_codes":VOLUSIA_ZIPS,"email":"TKent@volusia.org"},
    {"name":"David Santiago","title":"County Council Member, District 5","party":"R","level":"Local","state":"FL","district":"Volusia District 5","branch":"legislative","website":"https://www.volusia.org/government/county-council/","zip_codes":VOLUSIA_ZIPS,"email":"DSantiago@volusia.org"},

    # ── FLAGLER ──────────────────────────────────────────────────────────────
    {"name":"Mike Norris","title":"Mayor, City of Palm Coast","party":"R","level":"Local","state":"FL","district":"Palm Coast","branch":"executive","website":"https://www.palmcoastgov.com","zip_codes":FLAGLER_ZIPS},
    {"name":"Theresa Pontieri","title":"Vice Mayor, Palm Coast City Council District 2","party":"R","level":"Local","state":"FL","district":"Palm Coast District 2","branch":"legislative","website":"https://www.palmcoastgov.com","zip_codes":FLAGLER_ZIPS},
    {"name":"Ty Miller","title":"City Council Member, District 1","party":"R","level":"Local","state":"FL","district":"Palm Coast District 1","branch":"legislative","website":"https://www.palmcoastgov.com","zip_codes":FLAGLER_ZIPS},
    {"name":"David Sullivan","title":"City Council Member, District 3","party":"R","level":"Local","state":"FL","district":"Palm Coast District 3","branch":"legislative","website":"https://www.palmcoastgov.com","zip_codes":FLAGLER_ZIPS},
    {"name":"Charles Gambaro","title":"City Council Member, District 4","party":"R","level":"Local","state":"FL","district":"Palm Coast District 4","branch":"legislative","website":"https://www.palmcoastgov.com","zip_codes":FLAGLER_ZIPS},
    {"name":"Andy Dance","title":"Flagler County Commissioner, District 1","party":"N/A","level":"Local","state":"FL","district":"Flagler District 1","branch":"legislative","website":"https://www.flaglercounty.gov/government/county-commission","zip_codes":FLAGLER_ZIPS},
    {"name":"Donald O'Brien","title":"Flagler County Commissioner, District 2","party":"R","level":"Local","state":"FL","district":"Flagler District 2","branch":"legislative","website":"https://www.flaglercounty.gov/government/county-commission","zip_codes":FLAGLER_ZIPS},
    {"name":"Leann Pennington","title":"Flagler County Commissioner, District 3","party":"R","level":"Local","state":"FL","district":"Flagler District 3","branch":"legislative","website":"https://www.flaglercounty.gov/government/county-commission","zip_codes":FLAGLER_ZIPS},
    {"name":"Greg Hansen","title":"Flagler County Commissioner, District 4","party":"R","level":"Local","state":"FL","district":"Flagler District 4","branch":"legislative","website":"https://www.flaglercounty.gov/government/county-commission","zip_codes":FLAGLER_ZIPS},
    {"name":"Kim Carney","title":"Flagler County Commissioner, District 5","party":"R","level":"Local","state":"FL","district":"Flagler District 5","branch":"legislative","website":"https://www.flaglercounty.gov/government/county-commission","zip_codes":FLAGLER_ZIPS},
    {"name":"Rick Staly","title":"Flagler County Sheriff","party":"R","level":"Local","state":"FL","district":"Flagler County","branch":"executive","website":"https://www.flaglersheriff.com","zip_codes":FLAGLER_ZIPS},
]

def ingest():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    inserted = 0
    skipped = 0

    for o in OFFICIALS:
        try:
            existing = supabase.table("elected_officials") \
                .select("id") \
                .eq("name", o["name"]) \
                .eq("level", "Local") \
                .eq("state", "FL") \
                .execute()

            if existing.data:
                print(f"  ⏭ Skipped: {o['name']}")
                skipped += 1
            else:
                supabase.table("elected_officials").insert({
                    "name":      o["name"],
                    "title":     o["title"],
                    "party":     o["party"],
                    "level":     o["level"],
                    "state":     o["state"],
                    "district":  o["district"],
                    "zip_codes": o.get("zip_codes", ""),
                    "email":     o.get("email", ""),
                    "phone":     o.get("phone", ""),
                    "website":   o.get("website", ""),
                    "photo_url": o.get("photo_url", ""),
                    "branch":    o.get("branch", "legislative"),
                }).execute()
                print(f"  ✅ Inserted: {o['name']} — {o['title']}")
                inserted += 1
        except Exception as e:
            print(f"  ❌ Error for {o['name']}: {e}")

    print(f"\n{'='*50}")
    print(f"✅ Done! Inserted: {inserted} | Skipped: {skipped}")

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Set SUPABASE_URL and SUPABASE_KEY environment variables first.")
    else:
        ingest()
