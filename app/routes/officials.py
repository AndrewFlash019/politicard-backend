import os
import re
from fastapi import APIRouter, Depends, HTTPException, Query
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
    rows = response.data or []
    # Bills Sponsored belongs in the accountability scorecard, not the
    # generic "at a glance" metrics card on the profile.
    return [r for r in rows if (r.get("metric_name") or "").strip().lower() != "bills sponsored"]


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


_BIOGUIDE_RE = re.compile(r"congress\.gov/member/([A-Z]\d{6})", re.IGNORECASE)


def _slugify_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return slug


def _bills_view_url(name: str, level: str, source_url: str | None) -> str | None:
    """Build a congress.gov member URL with bioguide for federal officials.

    Bioguide is parsed out of the metric row's source_url because it's not
    stored on elected_officials. State/local officials return None.
    """
    if (level or "").lower() != "federal":
        return None
    if not source_url:
        return None
    m = _BIOGUIDE_RE.search(source_url)
    if not m:
        return None
    bioguide = m.group(1).upper()
    slug = _slugify_name(name)
    if not slug:
        return f"https://www.congress.gov/member/{bioguide}"
    return f"https://www.congress.gov/member/{slug}/{bioguide}"


@router.get("/{official_id}/accountability-scorecard")
def get_official_scorecard(official_id: int, db: Session = Depends(get_db)):
    if not _supabase:
        raise HTTPException(status_code=503, detail="Database not configured")

    official = db.execute(
        text("SELECT id, name, level FROM elected_officials WHERE id = :id"),
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
        entry = {
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
        }
        if key == "bills_sponsored":
            entry["view_bills_url"] = _bills_view_url(
                official.name, official.level, row.get("source_url")
            )
        metrics.append(entry)

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


@router.get("/{official_id}/expenditures")
def get_official_expenditures(official_id: int, db: Session = Depends(get_db)):
    """FEC schedule_b-derived top expenditures for the official's most recent
    cycle in campaign_finance. Returns the {top_payees, by_category,
    total_transactions} payload written by scripts/ingest_fec_disbursements.py.

    Returns 404 when the official exists but no top_expenditures has been
    ingested yet, so the frontend can hide the section cleanly.
    """
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
            .select("cycle,top_expenditures,source,source_url,last_updated")
            .eq("official_id", official_id)
            .order("cycle", desc=True)
            .limit(1)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = response.data or []
    if not rows or not rows[0].get("top_expenditures"):
        raise HTTPException(status_code=404, detail="No expenditure data for this official")

    row = rows[0]
    te = row["top_expenditures"]
    by_category = te.get("by_category") or {}
    total_spent = sum(float(v) for v in by_category.values() if v is not None)

    return {
        "cycle": row.get("cycle"),
        "top_payees": te.get("top_payees") or [],
        "by_category": by_category,
        "total_transactions": te.get("total_transactions"),
        "total_spent": total_spent,
        "source": row.get("source"),
        "source_url": row.get("source_url"),
        "last_updated": row.get("last_updated"),
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


@router.get("/{official_id}/legislative-activity")
def get_official_legislative_activity(
    official_id: int,
    type: str | None = Query(None, description="activity_type filter, e.g. bill_sponsored"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    official = db.execute(
        text("SELECT name FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not official:
        raise HTTPException(status_code=404, detail="Official not found")

    params: dict = {
        "name": official.name,
        "limit": limit,
        "offset": offset,
    }
    type_clause = ""
    if type:
        type_clause = "AND la.activity_type = :activity_type"
        params["activity_type"] = type

    # bill_sponsored: ingestion writes one row per status update, so the same
    # bill_number can appear 2-4 times. Collapse to one row per bill_number,
    # keeping the row with the most recent date.
    if type == "bill_sponsored":
        sql = f"""
            WITH dedup AS (
                SELECT DISTINCT ON (la.bill_number) la.id, la.bill_number,
                       la.title, la.date, la.status, la.plain_english_summary,
                       la.source_url
                FROM legislative_activity la
                WHERE LOWER(TRIM(la.official_name)) = LOWER(TRIM(:name))
                  {type_clause}
                  AND la.bill_number IS NOT NULL
                  AND TRIM(la.bill_number) <> ''
                ORDER BY la.bill_number, la.date DESC NULLS LAST, la.id DESC
            )
            SELECT d.id, d.bill_number, d.title, d.date, d.status,
                   d.plain_english_summary, d.source_url,
                   COALESCE(cv.support_count, 0) AS support_count,
                   COALESCE(cv.oppose_count, 0)  AS oppose_count,
                   COALESCE(cv.neutral_count, 0) AS neutral_count
            FROM dedup d
            LEFT JOIN (
                SELECT feed_card_id,
                       COUNT(*) FILTER (WHERE position = 'support') AS support_count,
                       COUNT(*) FILTER (WHERE position = 'oppose')  AS oppose_count,
                       COUNT(*) FILTER (WHERE position = 'neutral') AS neutral_count
                FROM constituent_votes
                GROUP BY feed_card_id
            ) cv ON cv.feed_card_id = d.id
            ORDER BY d.date DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """
    else:
        sql = f"""
            SELECT la.id, la.bill_number, la.title, la.date, la.status,
                   la.plain_english_summary, la.source_url,
                   COALESCE(cv.support_count, 0) AS support_count,
                   COALESCE(cv.oppose_count, 0)  AS oppose_count,
                   COALESCE(cv.neutral_count, 0) AS neutral_count
            FROM legislative_activity la
            LEFT JOIN (
                SELECT feed_card_id,
                       COUNT(*) FILTER (WHERE position = 'support') AS support_count,
                       COUNT(*) FILTER (WHERE position = 'oppose')  AS oppose_count,
                       COUNT(*) FILTER (WHERE position = 'neutral') AS neutral_count
                FROM constituent_votes
                GROUP BY feed_card_id
            ) cv ON cv.feed_card_id = la.id
            WHERE LOWER(TRIM(la.official_name)) = LOWER(TRIM(:name))
              {type_clause}
            ORDER BY la.date DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """

    rows = db.execute(text(sql), params).mappings().all()

    items = []
    for r in rows:
        d = dict(r)
        d["vote_counts"] = {
            "support": int(d.pop("support_count", 0) or 0),
            "oppose": int(d.pop("oppose_count", 0) or 0),
            "neutral": int(d.pop("neutral_count", 0) or 0),
        }
        items.append(d)

    return {
        "official_id": official_id,
        "activity_type": type,
        "limit": limit,
        "offset": offset,
        "items": items,
    }


@router.get("/{official_id}/my-votes")
def get_official_my_votes(
    official_id: int,
    user_id: str = Query(..., description="anon-* or authenticated user id"),
    db: Session = Depends(get_db),
):
    """Return all constituent_votes the given user has cast on bills tied to
    this official (feed_card_id maps to legislative_activity.id)."""
    rows = db.execute(
        text(
            """
            SELECT feed_card_id, position
            FROM constituent_votes
            WHERE official_id = :oid AND user_id = :uid
            """
        ),
        {"oid": official_id, "uid": user_id},
    ).mappings().all()

    return {
        "official_id": official_id,
        "user_id": user_id,
        "votes": [{"feed_card_id": r["feed_card_id"], "position": r["position"]} for r in rows],
    }


@router.get("/{official_id}/legislation")
def get_official_legislation(official_id: int, db: Session = Depends(get_db)):
    """Bills the official sponsored or cosponsored. Excludes Senate
    procedural placeholders (SS*/SP*) and rows missing a bill number."""
    official = db.execute(
        text("SELECT 1 FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not official:
        raise HTTPException(status_code=404, detail="Official not found")

    rows = db.execute(
        text(
            """
            SELECT la.id, la.bill_number, la.title, la.description, la.status,
                   la.vote_position, la.date, la.source, la.source_url,
                   la.activity_type, la.chamber, la.plain_english_summary,
                   la.full_text_url,
                   COALESCE(cv.support_count, 0) AS support_count,
                   COALESCE(cv.oppose_count, 0)  AS oppose_count,
                   COALESCE(cv.neutral_count, 0) AS neutral_count
            FROM legislative_activity la
            LEFT JOIN (
                SELECT feed_card_id,
                       COUNT(*) FILTER (WHERE position = 'support') AS support_count,
                       COUNT(*) FILTER (WHERE position = 'oppose')  AS oppose_count,
                       COUNT(*) FILTER (WHERE position = 'neutral') AS neutral_count
                FROM constituent_votes
                GROUP BY feed_card_id
            ) cv ON cv.feed_card_id = la.id
            WHERE la.official_id = :id
              AND la.activity_type IN ('bill_sponsored', 'bill_cosponsored')
              AND la.bill_number IS NOT NULL
              AND la.bill_number NOT LIKE 'SS%'
              AND la.bill_number NOT LIKE 'SP%'
            ORDER BY la.date DESC NULLS LAST
            LIMIT 50
            """
        ),
        {"id": official_id},
    ).mappings().all()

    items = []
    for r in rows:
        d = dict(r)
        d["vote_counts"] = {
            "support": int(d.pop("support_count", 0) or 0),
            "oppose": int(d.pop("oppose_count", 0) or 0),
            "neutral": int(d.pop("neutral_count", 0) or 0),
        }
        items.append(d)

    return items


@router.get("/{official_id}/committees")
def get_official_committees(official_id: int, db: Session = Depends(get_db)):
    """Committee assignments for the official. Includes both rows tagged
    activity_type='committee' and the Senate-procedural SS*/SP* placeholder
    rows that ingestion stores against committee codes."""
    official = db.execute(
        text("SELECT 1 FROM elected_officials WHERE id = :id"),
        {"id": official_id},
    ).first()
    if not official:
        raise HTTPException(status_code=404, detail="Official not found")

    rows = db.execute(
        text(
            """
            SELECT id, bill_number, title, description, status,
                   date, source, source_url, activity_type, chamber
            FROM legislative_activity
            WHERE official_id = :id
              AND (activity_type = 'committee'
                   OR bill_number LIKE 'SS%'
                   OR bill_number LIKE 'SP%')
            ORDER BY date DESC NULLS LAST
            """
        ),
        {"id": official_id},
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
