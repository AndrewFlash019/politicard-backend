import os
import re
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


_STATE_TITLE_KEYWORDS = (
    "governor",
    "lt. governor",
    "lieutenant governor",
    "attorney general",
    "cfo",
    "chief financial officer",
    "commissioner of agriculture",
    "us senator",
    "u.s. senator",
    "united states senator",
    "us representative",
    "u.s. representative",
    "united states representative",
    "state senator",
    "state representative",
    "senator",
    "representative",
)


def _strip_county_suffix(value: str) -> str:
    return re.sub(r"\s+county\s*$", "", value, flags=re.IGNORECASE).strip()


def _extract_city_from_title(title: str) -> str | None:
    m = re.search(
        r"mayor(?:,\s*(?:city|town|village)\s+of|\s+of)\s+(.+)",
        title,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    city = m.group(1).strip()
    # Drop trailing ", FL" or similar state annotations.
    city = re.sub(r",\s*(fl|florida)\s*$", "", city, flags=re.IGNORECASE).strip()
    return city or None


def _query_metric_rows(county: str, category: str | None) -> list[dict]:
    if not _supabase:
        raise HTTPException(status_code=503, detail="Database not configured")
    try:
        query = (
            _supabase.table("official_metrics")
            .select("metric_name,metric_value,metric_type,source,year,county,category")
            .ilike("county", county)
        )
        if category:
            query = query.eq("category", category)
        response = query.execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return response.data or []


@router.get("/{official_id}/metrics")
def get_official_metrics(official_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("SELECT id, name, title, level, district FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="Official not found")

    level = (row.level or "").lower()
    title = row.title or ""
    district = row.district or ""
    title_lower = title.lower()

    is_state_level = level == "federal" or any(
        kw in title_lower for kw in _STATE_TITLE_KEYWORDS
    )

    if is_state_level:
        return _query_metric_rows(county="Florida", category="State Government")

    if level == "local":
        category: str | None = None
        county: str | None = None

        if "mayor" in title_lower:
            category = "City Government"
            county = _extract_city_from_title(title)
        elif "sheriff" in title_lower:
            category = "County Government"
            county = _strip_county_suffix(district) if district else None
        elif "school board" in title_lower or "superintendent" in title_lower:
            category = "School Board"
            county = _strip_county_suffix(district) if district else None
        elif "county commissioner" in title_lower or "county commission" in title_lower:
            category = "County Commission"
            county = _strip_county_suffix(district) if district else None
        else:
            county = _strip_county_suffix(district) if district else None

        if not county:
            return []

        return _query_metric_rows(county=county, category=category)

    return []


@router.get("/{official_id}/accountability-scorecard")
def get_official_scorecard(official_id: int, db: Session = Depends(get_db)):
    if not _supabase:
        raise HTTPException(status_code=503, detail="Database not configured")

    official = db.execute(
        text("SELECT id, name FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not official:
        raise HTTPException(status_code=404, detail="Official not found")

    try:
        response = (
            _supabase.table("accountability_metrics")
            .select(
                "metric_key,metric_label,metric_value,metric_unit,"
                "performance_rating,benchmark_value,benchmark_label,"
                "year,source,source_url,notes"
            )
            .eq("official_id", official_id)
            .order("year", desc=True)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = response.data or []

    # For each metric_key keep the latest year of real data; fall back to the
    # latest no_data row only when no real data exists for that key.
    latest_by_key: dict[str, dict] = {}
    for row in rows:
        key = (row.get("metric_key") or "").strip()
        if not key:
            continue
        value = (row.get("metric_value") or "").strip()
        if not value:
            continue
        rating = row.get("performance_rating") or "no_data"
        existing = latest_by_key.get(key)
        if existing is None:
            latest_by_key[key] = row
            continue
        existing_rating = existing.get("performance_rating") or "no_data"
        # Real data beats no_data regardless of year (rows already DESC by year).
        if existing_rating == "no_data" and rating != "no_data":
            latest_by_key[key] = row

    metrics = []
    for key, row in latest_by_key.items():
        metrics.append({
            "key": key,
            "label": row.get("metric_label") or key,
            "value": row.get("metric_value") or "",
            "unit": row.get("metric_unit"),
            "rating": row.get("performance_rating") or "no_data",
            "benchmark_value": row.get("benchmark_value"),
            "benchmark_label": row.get("benchmark_label"),
            "year": row.get("year"),
            "source": row.get("source") or "",
            "source_url": row.get("source_url"),
            "notes": row.get("notes"),
        })

    metrics.sort(key=lambda m: (m["rating"] == "no_data", m["label"]))

    metrics_tracked = len(metrics)
    metrics_with_real_data = sum(1 for m in metrics if m["rating"] != "no_data")

    if metrics_tracked == 0:
        overall_rating = "Insufficient Data"
    else:
        strong = sum(1 for m in metrics if m["rating"] in ("excellent", "good"))
        concerning = sum(1 for m in metrics if m["rating"] in ("concerning", "poor"))
        unknown = metrics_tracked - metrics_with_real_data
        if strong >= metrics_tracked * 0.7:
            overall_rating = "Doing the Job"
        elif concerning >= metrics_tracked * 0.4:
            overall_rating = "Underperforming"
        elif unknown >= metrics_tracked * 0.5:
            overall_rating = "Insufficient Data"
        else:
            overall_rating = "Mixed Results"

    return {
        "official_id": official.id,
        "official_name": official.name,
        "overall_rating": overall_rating,
        "metrics_tracked": metrics_tracked,
        "metrics_with_real_data": metrics_with_real_data,
        "metrics": metrics,
    }


@router.get("/{official_id}/donors")
def get_official_donors(official_id: int, db: Session = Depends(get_db)):
    if not _supabase:
        raise HTTPException(status_code=503, detail="Database not configured")

    exists = db.execute(
        text("SELECT 1 FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not exists:
        raise HTTPException(status_code=404, detail="Official not found")

    try:
        response = (
            _supabase.table("campaign_finance")
            .select(
                "cycle,total_raised,total_spent,cash_on_hand,"
                "individual_contributions,pac_contributions,top_donors,top_pacs,"
                "source,source_url,last_updated"
            )
            .eq("official_id", official_id)
            .order("cycle", desc=True)
            .limit(1)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = response.data or []
    if not rows:
        return {}

    row = rows[0]
    total_raised = row.get("total_raised") or 0
    individual = row.get("individual_contributions") or 0
    pac = row.get("pac_contributions") or 0

    def _pct(part):
        if not total_raised:
            return 0
        return round(float(part) / float(total_raised) * 100, 2)

    return {
        "cycle": row.get("cycle"),
        "total_raised": row.get("total_raised"),
        "total_spent": row.get("total_spent"),
        "cash_on_hand": row.get("cash_on_hand"),
        "individual_contributions": row.get("individual_contributions"),
        "pac_contributions": row.get("pac_contributions"),
        "individual_percentage": _pct(individual),
        "pac_percentage": _pct(pac),
        "top_donors": row.get("top_donors") or [],
        "top_pacs": row.get("top_pacs") or [],
        "source": row.get("source"),
        "source_url": row.get("source_url"),
        "last_updated": row.get("last_updated"),
    }


@router.get("/{official_id}/spending")
def get_official_spending(official_id: int, db: Session = Depends(get_db)):
    if not _supabase:
        raise HTTPException(status_code=503, detail="Database not configured")

    official_row = db.execute(
        text("SELECT name FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not official_row:
        raise HTTPException(status_code=404, detail="Official not found")

    try:
        response = (
            _supabase.table("campaign_finance_spending")
            .select("total_spent,top_vendors,spending_by_category,cycle,updated_at")
            .eq("official_id", official_id)
            .order("cycle", desc=True)
            .limit(1)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = response.data or []
    if not rows:
        raise HTTPException(status_code=404, detail="No spending data for this official")

    row = rows[0]

    # Pull categorized vendor list (joins vendor_categories) so the UI can
    # render a category badge alongside each vendor.
    top_vendors_categorized: list[dict] = []
    try:
        cat_response = (
            _supabase.table("official_top_vendors_categorized")
            .select("vendor_name,amount,fec_purpose,category")
            .eq("official_name", official_row.name)
            .execute()
        )
        top_vendors_categorized = cat_response.data or []
    except Exception:
        top_vendors_categorized = []

    return {
        "total_spent": row.get("total_spent"),
        "top_vendors": row.get("top_vendors") or [],
        "top_vendors_categorized": top_vendors_categorized,
        "spending_by_category": row.get("spending_by_category") or [],
        "cycle": row.get("cycle"),
        "updated_at": row.get("updated_at"),
    }


@router.get("/{official_id}/funders-by-industry")
def get_official_funders_by_industry(official_id: int, db: Session = Depends(get_db)):
    if not _supabase:
        raise HTTPException(status_code=503, detail="Database not configured")

    exists = db.execute(
        text("SELECT 1 FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not exists:
        raise HTTPException(status_code=404, detail="Official not found")

    try:
        response = (
            _supabase.table("official_funders_by_industry")
            .select("category,category_total,funders")
            .eq("official_id", official_id)
            .order("category_total", desc=True)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return response.data or []


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
