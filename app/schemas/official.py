from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class OfficialBase(BaseModel):
    name: str
    title: str
    level: str
    party: Optional[str] = None
    state: str
    district: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    photo_url: Optional[str] = None

class OfficialCreate(OfficialBase):
    zip_codes: Optional[str] = None

class OfficialResponse(OfficialBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class OfficialsByZipResponse(BaseModel):
    zip_code: str
    federal: list[OfficialResponse] = []
    state: list[OfficialResponse] = []
    local: list[OfficialResponse] = []
    total_count: int = 0