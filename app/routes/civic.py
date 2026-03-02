from fastapi import APIRouter, HTTPException
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


@router.get("/officials/{zip_code}")
async def get_officials_by_zip(zip_code: str):
    """
    Fetch elected officials for a given ZIP code from Supabase.
    Returns officials where zip_codes contains the ZIP or is tagged ALL_FL.
    """
    try:
        # Query officials whose zip_codes field contains this ZIP
        response = supabase.table("elected_officials") \
            .select("*") \
            .or_(f"zip_codes.ilike.%{zip_code}%,zip_codes.eq.ALL_FL") \
            .eq("state", "FL") \
            .execute()

        officials_raw = response.data or []

        # Deduplicate by name+title (in case of overlapping zip_codes)
        seen = set()
        officials = []
        for o in officials_raw:
            key = f"{o.get('name')}|{o.get('title')}"
            if key not in seen:
                seen.add(key)
                officials.append(o)

        # Format to match what the frontend expects
        formatted = []
        for o in officials:
            formatted.append({
                "id": o.get("id"),
                "name": o.get("name", ""),
                "title": o.get("title", ""),
                "office": o.get("title", ""),
                "party": o.get("party", ""),
                "level": o.get("level", ""),
                "state": o.get("state", "FL"),
                "district": o.get("district", ""),
                "branch": o.get("branch", ""),
                "phone": o.get("phone"),
                "email": o.get("email"),
                "website": o.get("website"),
                "photo_url": o.get("photo_url"),
                "zip_codes": o.get("zip_codes", ""),
            })

        # Sort: federal first, then state, then local
        level_order = {"federal": 0, "state": 1, "local": 2}
        formatted.sort(key=lambda x: level_order.get(x["level"], 3))

        return {
            "zip_code": zip_code,
            "officials": formatted,
            "total": len(formatted),
            "state": "FL",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
