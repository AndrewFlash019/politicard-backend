from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_db
from app.dependencies.auth import get_current_user

router = APIRouter(prefix="/feed", tags=["feed"])

@router.get("/zip/{zip_code}")
def get_feed_by_zip(zip_code: str, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    if len(zip_code) != 5 or not zip_code.isdigit():
        raise HTTPException(status_code=400, detail="Invalid ZIP code format")

    # Get officials for this ZIP
    officials_query = text("""
        SELECT name FROM elected_officials
        WHERE zip_codes LIKE :zip
        AND state = 'FL'
    """)
    officials = db.execute(officials_query, {"zip": f"%{zip_code}%"}).fetchall()
    official_names = [row[0] for row in officials]

    if not official_names:
        return {"zip_code": zip_code, "items": [], "total_count": 0}

    # Get feed items for those officials
    placeholders = ", ".join([f":name_{i}" for i in range(len(official_names))])
    params = {"zip": zip_code}
    for i, name in enumerate(official_names):
        params[f"name_{i}"] = name

    feed_query = text(f"""
        SELECT id, official_name, official_id, state, level, item_type,
               title, description, vote_result, bill_id, bill_url,
               source, source_url, published_at, created_at
        FROM feed_items
        WHERE official_name IN ({placeholders})
        ORDER BY published_at DESC NULLS LAST
        LIMIT 50
    """)

    rows = db.execute(feed_query, params).fetchall()

    items = []
    for row in rows:
        items.append({
            "id": row[0],
            "official_name": row[1],
            "official_id": row[2],
            "state": row[3],
            "level": row[4],
            "item_type": row[5],
            "title": row[6],
            "description": row[7],
            "vote_result": row[8],
            "bill_id": row[9],
            "bill_url": row[10],
            "source": row[11],
            "source_url": row[12],
            "published_at": row[13].isoformat() if row[13] else None,
            "created_at": row[14].isoformat() if row[14] else None,
        })

    return {
        "zip_code": zip_code,
        "items": items,
        "total_count": len(items)
    }
