from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db

router = APIRouter(prefix="/feed", tags=["feed"])


def _relative_time(ts: Optional[datetime]) -> str:
    if not ts:
        return ""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - ts
    secs = int(delta.total_seconds())
    if secs < 60:
        return "just now"
    if secs < 3600:
        m = secs // 60
        return f"{m} minute{'s' if m != 1 else ''} ago"
    if secs < 86400:
        h = secs // 3600
        return f"{h} hour{'s' if h != 1 else ''} ago"
    days = secs // 86400
    if days == 1:
        return "yesterday"
    if days < 7:
        return f"{days} days ago"
    if days < 30:
        w = days // 7
        return f"{w} week{'s' if w != 1 else ''} ago"
    if days < 365:
        mo = days // 30
        return f"{mo} month{'s' if mo != 1 else ''} ago"
    return f"{days // 365} year{'s' if days // 365 != 1 else ''} ago"


def _serialize(row, last_visit: Optional[datetime]) -> dict:
    created = row[7]
    last_updated = row[8]
    is_new = bool(last_visit and created and created > last_visit)
    is_updated = bool(
        last_visit
        and last_updated
        and last_updated > last_visit
        and created
        and created <= last_visit
    )
    primary_ts = last_updated or created
    return {
        "id": row[0],
        "card_type": row[1],
        "title": row[2],
        "body": row[3],
        "icon": row[4],
        "official_name": row[5],
        "official_level": row[6],
        "created_at": created.isoformat() if created else None,
        "last_updated_at": last_updated.isoformat() if last_updated else None,
        "source": row[9],
        "source_url": row[10],
        "priority": row[11],
        "county": row[12],
        "event_date": row[13].isoformat() if row[13] else None,
        "bill_number": row[14],
        "related_metric_key": row[15],
        "group_key": row[16],
        "is_new": is_new,
        "is_updated": is_updated,
        "relative_time": _relative_time(primary_ts),
    }


CARD_COLS = """
    id, card_type, title, body, icon,
    official_name, official_level,
    created_at, last_updated_at,
    source, source_url, priority, county,
    event_date, bill_number, related_metric_key, group_key
"""


@router.get("/{zip_code}")
def get_feed_by_zip(
    zip_code: str,
    last_visit: Optional[str] = Query(None, description="ISO timestamp of user's last visit"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    if len(zip_code) != 5 or not zip_code.isdigit():
        raise HTTPException(status_code=400, detail="Invalid ZIP code format")

    last_visit_dt: Optional[datetime] = None
    if last_visit:
        try:
            last_visit_dt = datetime.fromisoformat(last_visit.replace("Z", "+00:00"))
            if last_visit_dt.tzinfo is None:
                last_visit_dt = last_visit_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            last_visit_dt = None

    # Officials whose ZIP list contains this ZIP
    officials_q = text(
        """
        SELECT name FROM elected_officials
        WHERE state = 'FL' AND zip_codes LIKE :zip_pat
        """
    )
    officials = db.execute(officials_q, {"zip_pat": f"%{zip_code}%"}).fetchall()
    official_names = [r[0] for r in officials]

    # County for this ZIP
    county_q = text("SELECT county FROM city_zips WHERE zip_codes LIKE :zip_pat LIMIT 1")
    county_row = db.execute(county_q, {"zip_pat": f"%{zip_code}%"}).fetchone()
    if not county_row:
        county_q2 = text("SELECT county FROM county_zips WHERE zip_codes LIKE :zip_pat LIMIT 1")
        county_row = db.execute(county_q2, {"zip_pat": f"%{zip_code}%"}).fetchone()
    county = county_row[0] if county_row else None

    # Today's brief
    brief_q = text(
        f"""
        SELECT {CARD_COLS}
        FROM feed_cards
        WHERE id = (
            SELECT feed_card_id FROM daily_brief_history
            WHERE zip_code = :zip AND brief_date = CURRENT_DATE
            ORDER BY id DESC LIMIT 1
        )
        """
    )
    brief_row = db.execute(brief_q, {"zip": zip_code}).fetchone()
    brief = _serialize(brief_row, last_visit_dt) if brief_row else None

    if not official_names and not county:
        return {
            "zip_code": zip_code,
            "today": {"brief": brief},
            "since_last_visit": [],
            "this_week": [],
            "coming_up": [],
            "your_officials": [],
            "active_card_count": 0,
            "last_refresh_at": datetime.now(timezone.utc).isoformat(),
        }

    name_placeholders = ", ".join([f":n{i}" for i in range(len(official_names))])
    name_params = {f"n{i}": n for i, n in enumerate(official_names)}
    name_filter = f"official_name IN ({name_placeholders})" if official_names else "FALSE"
    county_filter = "county = :county" if county else "FALSE"

    base_where = f"active = TRUE AND ({name_filter} OR {county_filter})"
    base_params = {**name_params, "county": county}

    # since_last_visit
    since_rows: list = []
    if last_visit_dt:
        slv_q = text(
            f"""
            SELECT {CARD_COLS}
            FROM feed_cards
            WHERE {base_where}
              AND (created_at > :lv OR last_updated_at > :lv)
            ORDER BY priority ASC, COALESCE(last_updated_at, created_at) DESC
            LIMIT :limit
            """
        )
        since_rows = db.execute(slv_q, {**base_params, "lv": last_visit_dt, "limit": limit}).fetchall()

    # this_week
    week_q = text(
        f"""
        SELECT {CARD_COLS}
        FROM feed_cards
        WHERE {base_where}
          AND (last_updated_at > NOW() - INTERVAL '7 days' OR created_at > NOW() - INTERVAL '7 days')
        ORDER BY priority ASC, COALESCE(last_updated_at, created_at) DESC
        LIMIT :limit OFFSET :offset
        """
    )
    week_rows = db.execute(week_q, {**base_params, "limit": limit, "offset": offset}).fetchall()

    # your_officials
    if official_names:
        yo_q = text(
            f"""
            SELECT {CARD_COLS}
            FROM feed_cards
            WHERE active = TRUE AND official_name IN ({name_placeholders})
            ORDER BY COALESCE(last_updated_at, created_at) DESC
            LIMIT :limit
            """
        )
        yo_rows = db.execute(yo_q, {**name_params, "limit": limit}).fetchall()
    else:
        yo_rows = []

    # coming_up: next 14 days
    cu_q = text(
        """
        SELECT id, title, description, event_type, event_date, jurisdiction,
               county, related_official_name, related_bill_number, source, source_url
        FROM coming_up_events
        WHERE active = TRUE
          AND event_date >= CURRENT_DATE
          AND event_date <= CURRENT_DATE + INTERVAL '14 days'
        ORDER BY event_date ASC
        LIMIT :limit
        """
    )
    cu_rows = db.execute(cu_q, {"limit": limit}).fetchall()
    coming_up = [
        {
            "id": r[0],
            "title": r[1],
            "description": r[2],
            "event_type": r[3],
            "event_date": r[4].isoformat() if r[4] else None,
            "jurisdiction": r[5],
            "county": r[6],
            "related_official_name": r[7],
            "related_bill_number": r[8],
            "source": r[9],
            "source_url": r[10],
        }
        for r in cu_rows
    ]

    # active_card_count
    count_q = text(f"SELECT COUNT(*) FROM feed_cards WHERE {base_where}")
    active_card_count = db.execute(count_q, base_params).scalar() or 0

    return {
        "zip_code": zip_code,
        "county": county,
        "today": {"brief": brief},
        "since_last_visit": [_serialize(r, last_visit_dt) for r in since_rows],
        "this_week": [_serialize(r, last_visit_dt) for r in week_rows],
        "your_officials": [_serialize(r, last_visit_dt) for r in yo_rows],
        "coming_up": coming_up,
        "active_card_count": active_card_count,
        "last_refresh_at": datetime.now(timezone.utc).isoformat(),
    }


# Legacy alias preserved for existing clients
@router.get("/zip/{zip_code}")
def get_feed_by_zip_legacy(zip_code: str, db: Session = Depends(get_db)):
    return get_feed_by_zip(zip_code, last_visit=None, limit=50, offset=0, db=db)
