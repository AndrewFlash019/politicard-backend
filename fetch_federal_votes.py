import requests
import os
import time
from supabase import create_client
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
CONGRESS_API_KEY = os.environ.get("CONGRESS_API_KEY", "")

# Florida federal officials to track
FL_FEDERAL_OFFICIALS = [
    {"name": "Ashley Moody", "bioguide_id": "M001217", "chamber": "senate"},
    {"name": "Rick Scott", "bioguide_id": "S001217", "chamber": "senate"},
    {"name": "Matt Gaetz", "bioguide_id": "G000578", "chamber": "house"},
    {"name": "Neal Dunn", "bioguide_id": "D000628", "chamber": "house"},
    {"name": "Kat Cammack", "bioguide_id": "C001131", "chamber": "house"},
    {"name": "Aaron Bean", "bioguide_id": "B001315", "chamber": "house"},
    {"name": "Randy Fine", "bioguide_id": "F000478", "chamber": "house"},
    {"name": "Gus Bilirakis", "bioguide_id": "B001257", "chamber": "house"},
    {"name": "Bill Posey", "bioguide_id": "P000599", "chamber": "house"},
    {"name": "Darren Soto", "bioguide_id": "S001200", "chamber": "house"},
    {"name": "Daniel Webster", "bioguide_id": "W000806", "chamber": "house"},
    {"name": "Gus Bilirakis", "bioguide_id": "B001257", "chamber": "house"},
    {"name": "Kathy Castor", "bioguide_id": "C001066", "chamber": "house"},
    {"name": "Vern Buchanan", "bioguide_id": "B001260", "chamber": "house"},
    {"name": "Greg Steube", "bioguide_id": "S001214", "chamber": "house"},
    {"name": "Scott Franklin", "bioguide_id": "F000476", "chamber": "house"},
    {"name": "Byron Donalds", "bioguide_id": "D000032", "chamber": "house"},
    {"name": "Sheila Cherfilus-McCormick", "bioguide_id": "C001128", "chamber": "house"},
    {"name": "Brian Mast", "bioguide_id": "M001199", "chamber": "house"},
    {"name": "Lois Frankel", "bioguide_id": "F000462", "chamber": "house"},
    {"name": "Jared Moskowitz", "bioguide_id": "M001219", "chamber": "house"},
    {"name": "Debbie Wasserman Schultz", "bioguide_id": "W000797", "chamber": "house"},
    {"name": "Mario Diaz-Balart", "bioguide_id": "D000600", "chamber": "house"},
    {"name": "Maria Elvira Salazar", "bioguide_id": "S001214", "chamber": "house"},
    {"name": "Carlos Gimenez", "bioguide_id": "G000591", "chamber": "house"},
]

def fetch_member_votes(bioguide_id, name, limit=20):
    """Fetch recent votes for a member from Congress.gov API."""
    url = f"https://api.congress.gov/v3/member/{bioguide_id}/votes"
    params = {
        "api_key": CONGRESS_API_KEY,
        "limit": limit,
        "format": "json",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        votes = data.get("votes", [])
        print(f"  {name}: {len(votes)} votes found")
        return votes
    except Exception as e:
        print(f"  {name}: Error — {e}")
        return []

def fetch_sponsored_legislation(bioguide_id, name, limit=10):
    """Fetch bills sponsored by a member."""
    url = f"https://api.congress.gov/v3/member/{bioguide_id}/sponsored-legislation"
    params = {
        "api_key": CONGRESS_API_KEY,
        "limit": limit,
        "format": "json",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        bills = data.get("sponsoredLegislation", [])
        print(f"  {name}: {len(bills)} sponsored bills found")
        return bills
    except Exception as e:
        print(f"  {name}: Error — {e}")
        return []

def ingest_votes(supabase, official, votes):
    """Insert vote records into feed_items."""
    inserted = 0
    for vote in votes:
        try:
            bill = vote.get("bill", {})
            bill_id = bill.get("number", "") if bill else ""
            congress = bill.get("congress", "") if bill else ""
            bill_type = bill.get("type", "") if bill else ""
            full_bill_id = f"{bill_type}{bill_id}-{congress}" if bill_id else ""

            vote_position = vote.get("memberVoted", vote.get("votePosition", ""))
            description = vote.get("description") or vote.get("bill", {}).get("title", "") or "Voted on legislation"
            title = f"{official['name']} voted {vote_position.upper() if vote_position else '?'} on {bill_type or 'bill'} {bill_id or '—'}"

            date_str = vote.get("date") or vote.get("actionDate", "")
            try:
                pub_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None
            except:
                pub_date = None

            # Check for duplicate
            existing = supabase.table("feed_items") \
                .select("id") \
                .eq("official_name", official["name"]) \
                .eq("bill_id", full_bill_id) \
                .eq("item_type", "vote") \
                .execute()

            if existing.data:
                continue

            supabase.table("feed_items").insert({
                "official_name": official["name"],
                "official_id": official["bioguide_id"],
                "state": "FL",
                "level": "federal",
                "item_type": "vote",
                "title": title,
                "description": description[:500] if description else "",
                "vote_result": vote_position or "",
                "bill_id": full_bill_id,
                "bill_url": f"https://www.congress.gov/bill/{congress}th-congress/{bill_type.lower()}-bill/{bill_id}" if bill_id else "",
                "source": "congress.gov",
                "source_url": f"https://www.congress.gov/member/{official['bioguide_id']}",
                "published_at": pub_date.isoformat() if pub_date else None,
            }).execute()
            inserted += 1
        except Exception as e:
            print(f"    Error inserting vote: {e}")
    return inserted

def ingest_bills(supabase, official, bills):
    """Insert sponsored bill records into feed_items."""
    inserted = 0
    for bill in bills:
        try:
            bill_id = bill.get("number", "")
            bill_type = bill.get("type", "")
            congress = bill.get("congress", "")
            full_bill_id = f"{bill_type}{bill_id}-{congress}"
            title_text = bill.get("title", "Legislation")[:200]
            title = f"{official['name']} sponsored {bill_type or 'bill'} {bill_id}: {title_text}"

            date_str = bill.get("introducedDate", "")
            try:
                pub_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) if date_str else None
            except:
                pub_date = None

            existing = supabase.table("feed_items") \
                .select("id") \
                .eq("official_name", official["name"]) \
                .eq("bill_id", full_bill_id) \
                .eq("item_type", "legislation") \
                .execute()

            if existing.data:
                continue

            supabase.table("feed_items").insert({
                "official_name": official["name"],
                "official_id": official["bioguide_id"],
                "state": "FL",
                "level": "federal",
                "item_type": "legislation",
                "title": title,
                "description": title_text,
                "bill_id": full_bill_id,
                "bill_url": f"https://www.congress.gov/bill/{congress}th-congress/{bill_type.lower()}-bill/{bill_id}" if bill_id else "",
                "source": "congress.gov",
                "source_url": f"https://www.congress.gov/member/{official['bioguide_id']}",
                "published_at": pub_date.isoformat() if pub_date else None,
            }).execute()
            inserted += 1
        except Exception as e:
            print(f"    Error inserting bill: {e}")
    return inserted

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    total_inserted = 0

    for official in FL_FEDERAL_OFFICIALS:
        print(f"\n🏛️ {official['name']}")

        votes = fetch_member_votes(official["bioguide_id"], official["name"])
        total_inserted += ingest_votes(supabase, official, votes)
        time.sleep(0.5)

        bills = fetch_sponsored_legislation(official["bioguide_id"], official["name"])
        total_inserted += ingest_bills(supabase, official, bills)
        time.sleep(0.5)

    print(f"\n{'='*50}")
    print(f"✅ Done! Total inserted: {total_inserted}")

if __name__ == "__main__":
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Set SUPABASE_URL and SUPABASE_KEY first.")
    elif not CONGRESS_API_KEY:
        print("❌ Set CONGRESS_API_KEY first.")
    else:
        main()
