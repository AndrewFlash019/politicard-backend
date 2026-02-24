from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, Text
from sqlalchemy.sql import func
from app.database import Base

class TypologyResult(Base):
    __tablename__ = "typology_results"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    economic_score = Column(Float, nullable=False)
    social_score = Column(Float, nullable=False)
    engagement_level = Column(String(10), nullable=False)
    typology_label = Column(String(50), nullable=True)
    responses = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())