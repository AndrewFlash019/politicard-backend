from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.schemas.official import OfficialCreate, OfficialResponse, OfficialsByZipResponse
from app.services.official import get_officials_by_zip, create_official
from app.dependencies.auth import get_current_user

router = APIRouter(prefix="/officials", tags=["officials"])

@router.get("/zip/{zip_code}", response_model=OfficialsByZipResponse)
def lookup_by_zip(zip_code: str, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    if len(zip_code) != 5 or not zip_code.isdigit():
        raise HTTPException(status_code=400, detail="Invalid ZIP code format")
    result = get_officials_by_zip(db, zip_code)
    return result

@router.post("/", response_model=OfficialResponse)
def add_official(official: OfficialCreate, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    official_data = official.model_dump()
    return create_official(db, official_data)