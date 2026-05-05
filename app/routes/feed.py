import hashlib
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db

router = APIRouter(prefix="/feed", tags=["feed"])
constituent_router = APIRouter(prefix="/constituent-votes", tags=["constituent-votes"])
engagement_router = APIRouter(prefix="/users", tags=["engagement"])
official_alignment_router = APIRouter(prefix="/officials", tags=["alignment"])


# Civic-engagement levels: (min_votes, label, emoji)
ENGAGEMENT_LEVELS = [
    (0,  "New Voter",          "🌱"),
    (1,  "Poll Voter",         "🗳️"),
    (5,  "Civic Voice",        "📣"),
    (8,  "Active Citizen",     "🏛️"),
    (10, "Civic Champion",     "🏆"),
    (15, "Democracy Defender", "🛡️"),
]


def _engagement_level_for(votes: int) -> dict:
    current_idx = 0
    for i, (mv, _, _) in enumerate(ENGAGEMENT_LEVELS):
        if votes >= mv:
            current_idx = i
    cur_min, cur_name, cur_emoji = ENGAGEMENT_LEVELS[current_idx]
    nxt = ENGAGEMENT_LEVELS[current_idx + 1] if current_idx + 1 < len(ENGAGEMENT_LEVELS) else None
    if nxt:
        next_min, next_name, _ = nxt
        votes_to_next = max(0, next_min - votes)
    else:
        next_name, votes_to_next = None, 0
    return {
        "level_name": cur_name,
        "level_emoji": cur_emoji,
        "level_threshold": cur_min,
        "next_level_name": next_name,
        "votes_to_next_level": votes_to_next,
    }


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


# ---------------------------------------------------------------------------
# Stream feed: chronological mix of every legislative_activity row tied to an
# official representing the ZIP, with aggregate constituent_votes counts.
# Powers the infinite-scroll feed UI.
# ---------------------------------------------------------------------------
@router.get("/{zip_code}/stream")
def get_feed_stream(
    zip_code: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    if len(zip_code) != 5 or not zip_code.isdigit():
        raise HTTPException(status_code=400, detail="Invalid ZIP code format")

    # Officials representing this ZIP
    off_rows = db.execute(
        text(
            """
            SELECT id, name, level
            FROM elected_officials
            WHERE zip_codes LIKE :zip_pat
            """
        ),
        {"zip_pat": f"%{zip_code}%"},
    ).mappings().all()
    if not off_rows:
        return {"zip_code": zip_code, "items": [], "limit": limit, "offset": offset}

    official_ids = [o["id"] for o in off_rows]
    name_by_id = {o["id"]: o["name"] for o in off_rows}
    level_by_id = {o["id"]: o["level"] for o in off_rows}

    # Build a parametrized IN list — psycopg2 accepts tuple binding
    placeholders = ", ".join([f":id{i}" for i in range(len(official_ids))])
    params = {f"id{i}": v for i, v in enumerate(official_ids)}
    params["lim"] = limit
    params["off"] = offset

    rows = db.execute(
        text(
            f"""
            SELECT la.id, la.official_id, la.activity_type, la.bill_number,
                   la.title, la.description, la.plain_english_summary,
                   la.status, la.vote_position, la.date,
                   la.source, la.source_url, la.full_text_url,
                   la.chamber,
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
            WHERE la.official_id IN ({placeholders})
            ORDER BY la.date DESC NULLS LAST, la.id DESC
            LIMIT :lim OFFSET :off
            """
        ),
        params,
    ).mappings().all()

    items = []
    for r in rows:
        d = dict(r)
        d["official_name"] = name_by_id.get(d["official_id"])
        d["official_level"] = level_by_id.get(d["official_id"])
        # Promote a date-only column to ISO string for JSON
        if d.get("date") and hasattr(d["date"], "isoformat"):
            d["date"] = d["date"].isoformat()
        items.append(d)

    return {
        "zip_code": zip_code,
        "items": items,
        "limit": limit,
        "offset": offset,
        "next_offset": offset + len(items) if len(items) == limit else None,
    }


# ---------------------------------------------------------------------------
# Constituent vote: anonymous per-device support/oppose/neutral on a feed card.
# user_id is derived from request IP+UA so a single device is stable across
# refreshes without requiring auth, and unique-per-card enforcement remains
# possible at the row level.
# ---------------------------------------------------------------------------
class ConstituentVoteIn(BaseModel):
    official_id: int
    feed_card_id: int
    position: str = Field(..., description="support | oppose | neutral")
    user_id: Optional[str] = None


_VALID_POSITIONS = {"support", "oppose", "neutral"}


def _anon_user_id(request: Request) -> str:
    fwd = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    ip = fwd or (request.client.host if request.client else "unknown")
    ua = request.headers.get("user-agent", "")
    return "anon-" + hashlib.sha256(f"{ip}|{ua}".encode()).hexdigest()[:24]


@constituent_router.post("")
@constituent_router.post("/")
def cast_constituent_vote(
    payload: ConstituentVoteIn,
    request: Request,
    db: Session = Depends(get_db),
):
    pos = (payload.position or "").lower().strip()
    if pos not in _VALID_POSITIONS:
        raise HTTPException(status_code=400, detail="position must be support|oppose|neutral")

    user_id = (payload.user_id or _anon_user_id(request))[:120]

    # Upsert: one row per (user_id, feed_card_id); a re-vote replaces the prior position
    db.execute(
        text(
            """
            INSERT INTO constituent_votes (user_id, feed_card_id, official_id, position, created_at)
            VALUES (:uid, :fid, :oid, :pos, NOW())
            ON CONFLICT (user_id, feed_card_id) DO UPDATE
              SET position = EXCLUDED.position,
                  official_id = EXCLUDED.official_id,
                  created_at = NOW()
            """
        ),
        {"uid": user_id, "fid": payload.feed_card_id, "oid": payload.official_id, "pos": pos},
    )
    db.commit()

    counts = db.execute(
        text(
            """
            SELECT
              COUNT(*) FILTER (WHERE position = 'support') AS support_count,
              COUNT(*) FILTER (WHERE position = 'oppose')  AS oppose_count,
              COUNT(*) FILTER (WHERE position = 'neutral') AS neutral_count
            FROM constituent_votes WHERE feed_card_id = :fid
            """
        ),
        {"fid": payload.feed_card_id},
    ).mappings().first() or {}

    return {
        "feed_card_id": payload.feed_card_id,
        "your_position": pos,
        "support_count": counts.get("support_count", 0),
        "oppose_count": counts.get("oppose_count", 0),
        "neutral_count": counts.get("neutral_count", 0),
    }


# ---------------------------------------------------------------------------
# Civic engagement: total votes + level progression for a user_id
# ---------------------------------------------------------------------------
@engagement_router.get("/{user_id}/engagement")
def get_user_engagement(user_id: str, db: Session = Depends(get_db)):
    row = db.execute(
        text("SELECT COUNT(*) AS n FROM constituent_votes WHERE user_id = :uid"),
        {"uid": user_id},
    ).mappings().first() or {}
    total = int(row.get("n") or 0)
    level = _engagement_level_for(total)
    return {
        "user_id": user_id,
        "total_votes": total,
        **level,
    }


# ---------------------------------------------------------------------------
# Per-official alignment: how often did this user vote the same way as the
# official's recorded vote_position. Maps support↔Yea, oppose↔Nay; neutral
# rows do not count toward agree/disagree but are reflected in total_compared.
# ---------------------------------------------------------------------------
@official_alignment_router.get("/{official_id}/alignment")
def get_official_alignment(
    official_id: int,
    user_id: str = Query(..., description="anon-* or authenticated user id"),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            """
            SELECT cv.position AS user_position, la.vote_position AS official_position
            FROM constituent_votes cv
            JOIN legislative_activity la ON la.id = cv.feed_card_id
            WHERE cv.user_id = :uid
              AND la.official_id = :oid
              AND la.activity_type = 'vote'
              AND la.vote_position IS NOT NULL
            """
        ),
        {"uid": user_id, "oid": official_id},
    ).mappings().all()

    agree = 0
    disagree = 0
    skipped = 0
    for r in rows:
        u = (r["user_position"] or "").lower()
        o = (r["official_position"] or "").lower()
        is_yea = o.startswith("y") or o == "aye"
        is_nay = o.startswith("n") and not o.startswith("not")
        if u == "support":
            if is_yea: agree += 1
            elif is_nay: disagree += 1
            else: skipped += 1
        elif u == "oppose":
            if is_nay: agree += 1
            elif is_yea: disagree += 1
            else: skipped += 1
        else:
            skipped += 1

    total_compared = agree + disagree
    pct = round((agree / total_compared) * 100, 1) if total_compared else None

    return {
        "official_id": official_id,
        "user_id": user_id,
        "agree_count": agree,
        "disagree_count": disagree,
        "skipped_count": skipped,
        "total_compared": total_compared,
        "alignment_pct": pct,
    }
