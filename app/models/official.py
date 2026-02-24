from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from app.database import Base

class ElectedOfficial(Base):
    __tablename__ = "elected_officials"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    level = Column(String(20), nullable=False, index=True)
    branch = Column(String(20), nullable=False, index=True)
    party = Column(String(50), nullable=True)
    state = Column(String(2), nullable=False, index=True)
    district = Column(String(50), nullable=True)
    zip_codes = Column(Text, nullable=True)
    email = Column(String, nullable=True)
    phone = Column(String(20), nullable=True)
    website = Column(String, nullable=True)
    photo_url = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())