import os
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from supabase import create_client
from dotenv import load_dotenv
from app.database import get_db
from app.schemas.official import OfficialCreate, OfficialResponse, OfficialsByZipResponse
from app.services.official import get_officials_by_zip, create_official
from app.dependencies.auth import get_current_user

load_dotenv()

router = APIRouter(prefix="/officials", tags=["officials"])
metrics_router = APIRouter(prefix="/metrics", tags=["metrics"])

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

_supabase = None
if SUPABASE_URL and SUPABASE_SERVICE_KEY:
    _supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


@router.get("/zip/{zip_code}", response_model=OfficialsByZipResponse)
def lookup_by_zip(zip_code: str, db: Session = Depends(get_db)):
    if len(zip_code) != 5 or not zip_code.isdigit():
        raise HTTPException(status_code=400, detail="Invalid ZIP code format")
    result = get_officials_by_zip(db, zip_code)
    return result

@router.post("/", response_model=OfficialResponse)
def add_official(official: OfficialCreate, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    official_data = official.model_dump()
    return create_official(db, official_data)


@router.get("/{official_id}/legislation")
def get_official_legislation(official_id: int, db: Session = Depends(get_db)):
    official = db.execute(
        text("SELECT name FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not official:
        raise HTTPException(status_code=404, detail="Official not found")

    rows = db.execute(
        text(
            """
            SELECT id, bill_number, title, description, status, vote_position,
                   date, source, source_url, activity_type, chamber
            FROM legislative_activity
            WHERE LOWER(TRIM(official_name)) = LOWER(TRIM(:name))
            ORDER BY date DESC NULLS LAST
            """
        ),
        {"name": official.name},
    ).mappings().all()

    return [dict(row) for row in rows]


def _fetch_metrics_for_county(county_name: str) -> dict:
    if not _supabase:
        raise HTTPException(status_code=503, detail="Database not configured")
    try:
        response = _supabase.table("official_metrics") \
            .select("*") \
            .ilike("county", county_name) \
            .execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = response.data or []
    grouped: dict[str, list] = {}
    for row in rows:
        category = row.get("category") or "Other"
        grouped.setdefault(category, []).append({
            "name": row.get("metric_name", ""),
            "value": row.get("metric_value", ""),
            "type": row.get("metric_type", "text"),
            "source": row.get("source"),
            "year": row.get("year"),
        })

    return {"county": county_name, "metrics": grouped}


@metrics_router.get("/county/{county_name}")
def get_metrics_by_county(county_name: str):
    return _fetch_metrics_for_county(county_name)


@metrics_router.get("/zip/{zip_code}")
def get_metrics_by_zip(zip_code: str):
    if len(zip_code) != 5 or not zip_code.isdigit():
        raise HTTPException(status_code=400, detail="Invalid ZIP code format")
    if not _supabase:
        raise HTTPException(status_code=503, detail="Database not configured")
    try:
        lookup = _supabase.table("county_zips") \
            .select("county,zip_codes") \
            .ilike("zip_codes", f"%{zip_code}%") \
            .execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = lookup.data or []
    county_name = None
    for row in rows:
        zips_raw = row.get("zip_codes") or ""
        zips = {z.strip() for z in zips_raw.replace(";", ",").split(",") if z.strip()}
        if zip_code in zips:
            county_name = row.get("county")
            break

    if not county_name:
        raise HTTPException(status_code=404, detail=f"No county found for ZIP {zip_code}")

    return _fetch_metrics_for_county(county_name)
