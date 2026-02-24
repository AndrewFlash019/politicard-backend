from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.ai import (
    classify_typology,
    summarize_official,
    analyze_sentiment,
    recommend_content,
    moderate_discussion,
)
from app.dependencies.auth import get_current_user

router = APIRouter(prefix="/ai", tags=["ai"])

class TypologyRequest(BaseModel):
    responses: dict

class SentimentRequest(BaseModel):
    article_text: str

class ModerateRequest(BaseModel):
    message: str

class SummarizeRequest(BaseModel):
    official_data: dict

@router.post("/typology")
def ai_classify_typology(request: TypologyRequest, current_user=Depends(get_current_user)):
    try:
        result = classify_typology(request.responses)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/summarize-official")
def ai_summarize_official(request: SummarizeRequest, current_user=Depends(get_current_user)):
    try:
        result = summarize_official(request.official_data)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sentiment")
def ai_analyze_sentiment(request: SentimentRequest, current_user=Depends(get_current_user)):
    try:
        result = analyze_sentiment(request.article_text)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/moderate")
def ai_moderate_discussion(request: ModerateRequest, current_user=Depends(get_current_user)):
    try:
        result = moderate_discussion(request.message)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))