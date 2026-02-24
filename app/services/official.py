from sqlalchemy.orm import Session
from app.models.official import ElectedOfficial

def get_officials_by_zip(db: Session, zip_code: str):
    officials = db.query(ElectedOfficial).filter(
        ElectedOfficial.zip_codes.contains(zip_code)
    ).all()

    federal = [o for o in officials if o.level == "federal"]
    state = [o for o in officials if o.level == "state"]
    local = [o for o in officials if o.level == "local"]

    return {
        "zip_code": zip_code,
        "federal": federal,
        "state": state,
        "local": local,
        "total_count": len(officials)
    }

def create_official(db: Session, official_data: dict):
    official = ElectedOfficial(**official_data)
    db.add(official)
    db.commit()
    db.refresh(official)
    return official