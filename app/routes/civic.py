from fastapi import APIRouter, HTTPException
import httpx
import os
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
OPENSTATES_API_KEY = os.getenv("OPENSTATES_API_KEY")


async def get_lat_lng_from_zip(zip_code: str) -> tuple:
    """Convert ZIP code to lat/lng using free Census geocoder."""
    url = "https://nominatim.openstreetmap.org/search"
    params = {"postalcode": zip_code, "country": "US", "format": "json", "limit": 1}
    headers = {"User-Agent": "PolitiCard/1.0"}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, headers=headers)
        data = response.json()
        if not data:
            raise HTTPException(status_code=404, detail="ZIP code not found")
        return float(data[0]["lat"]), float(data[0]["lon"])


async def get_federal_officials(zip_code: str) -> list:
    """Get federal officials (House + Senate) by ZIP using Congress.gov API."""
    results = []
    async with httpx.AsyncClient() as client:
        # Get House members by ZIP
        url = "https://api.congress.gov/v3/member"
        params = {
            "api_key": CONGRESS_API_KEY,
            "zipCode": zip_code,
            "currentMember": True,
            "limit": 10,
        }
        response = await client.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            for member in data.get("members", []):
                results.append({
                    "office": member.get("terms", {}).get("item", [{}])[-1].get("chamber", "Congress"),
                    "name": member.get("name", ""),
                    "party": member.get("partyName", ""),
                    "state": member.get("state", ""),
                    "district": member.get("district", ""),
                    "phone": None,
                    "website": member.get("officialWebsiteUrl", None),
                    "photo_url": member.get("depiction", {}).get("imageUrl", None),
                    "level": "federal",
                })
    return results


async def get_state_officials(lat: float, lng: float) -> list:
    """Get state officials by lat/lng using OpenStates API."""
    results = []
    url = "https://v3.openstates.org/people.geo"
    params = {"lat": lat, "lng": lng}
    headers = {"X-API-KEY": OPENSTATES_API_KEY}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for person in data.get("results", []):
                results.append({
                    "office": person.get("current_role", {}).get("title", "State Official"),
                    "name": person.get("name", ""),
                    "party": person.get("party", ""),
                    "state": person.get("current_role", {}).get("state", ""),
                    "district": person.get("current_role", {}).get("district", ""),
                    "phone": None,
                    "website": person.get("openstates_url", None),
                    "photo_url": person.get("image", None),
                    "level": "state",
                })
    return results


@router.get("/officials/{zip_code}")
async def get_officials_by_zip(zip_code: str):
    try:
        lat, lng = await get_lat_lng_from_zip(zip_code)
        federal = await get_federal_officials(zip_code)
        state = await get_state_officials(lat, lng)
        all_officials = state + federal  # State first per PolitiCard priority
        return {"zip_code": zip_code, "officials": all_officials}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))