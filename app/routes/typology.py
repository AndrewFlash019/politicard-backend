"""Typology quiz: 10-question political-identity assessment.

Two endpoints:
  GET  /typology/questions  – returns the hardcoded question set
  POST /typology/submit     – scores answers, persists, returns typology

Schema notes: the existing `typology_results` table has an integer `user_id`
column (FK to users.id) plus an `engagement_level` field used by an older
authenticated path. To keep this anonymous-first quiz from breaking that
relationship we add `anon_user_id TEXT`, `scores JSONB`, `typology TEXT` via
a migration and write to those columns here.
"""

from __future__ import annotations

import json
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db

router = APIRouter(prefix="/typology", tags=["typology"])


# ---------------------------------------------------------------------------
# Hardcoded question set. Each option carries a numeric value in [-2, +2].
# Questions 1,2,3,5,9,10 contribute to the economic axis; 4,6,7,8 to social.
# Negative = progressive/left, positive = conservative/right.
# ---------------------------------------------------------------------------
QUESTIONS: list[dict] = [
    {
        "id": 1,
        "axis": "economic",
        "text": "The federal government should play a larger role in solving major problems.",
        "options": [
            {"text": "Strongly agree",    "value": -2},
            {"text": "Agree",             "value": -1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value":  1},
            {"text": "Strongly disagree", "value":  2},
        ],
    },
    {
        "id": 2,
        "axis": "economic",
        "text": "Taxes on high earners and large corporations should be increased to fund public services.",
        "options": [
            {"text": "Strongly agree",    "value": -2},
            {"text": "Agree",             "value": -1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value":  1},
            {"text": "Strongly disagree", "value":  2},
        ],
    },
    {
        "id": 3,
        "axis": "economic",
        "text": "Healthcare should be guaranteed by the government for all citizens.",
        "options": [
            {"text": "Strongly agree",    "value": -2},
            {"text": "Agree",             "value": -1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value":  1},
            {"text": "Strongly disagree", "value":  2},
        ],
    },
    {
        "id": 4,
        "axis": "social",
        "text": "Stricter environmental regulations are necessary even if they slow economic growth.",
        "options": [
            {"text": "Strongly agree",    "value": -2},
            {"text": "Agree",             "value": -1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value":  1},
            {"text": "Strongly disagree", "value":  2},
        ],
    },
    {
        "id": 5,
        "axis": "economic",
        "text": "Most social safety-net programs should be expanded.",
        "options": [
            {"text": "Strongly agree",    "value": -2},
            {"text": "Agree",             "value": -1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value":  1},
            {"text": "Strongly disagree", "value":  2},
        ],
    },
    {
        "id": 6,
        "axis": "social",
        "text": "Stricter gun control laws would make our communities safer.",
        "options": [
            {"text": "Strongly agree",    "value": -2},
            {"text": "Agree",             "value": -1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value":  1},
            {"text": "Strongly disagree", "value":  2},
        ],
    },
    {
        "id": 7,
        "axis": "social",
        "text": "Immigration enforcement should be tightened at our borders.",
        "options": [
            {"text": "Strongly agree",    "value":  2},
            {"text": "Agree",             "value":  1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value": -1},
            {"text": "Strongly disagree", "value": -2},
        ],
    },
    {
        "id": 8,
        "axis": "social",
        "text": "The U.S. should generally avoid involvement in foreign conflicts.",
        "options": [
            {"text": "Strongly agree",    "value": -1},
            {"text": "Agree",             "value": -1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value":  1},
            {"text": "Strongly disagree", "value":  1},
        ],
    },
    {
        "id": 9,
        "axis": "economic",
        "text": "Free trade agreements benefit American workers more than they hurt them.",
        "options": [
            {"text": "Strongly agree",    "value":  1},
            {"text": "Agree",             "value":  1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value": -1},
            {"text": "Strongly disagree", "value": -1},
        ],
    },
    {
        "id": 10,
        "axis": "economic",
        "text": "Reducing the national debt should be a top priority, even if it means cutting popular programs.",
        "options": [
            {"text": "Strongly agree",    "value":  2},
            {"text": "Agree",             "value":  1},
            {"text": "Neutral",           "value":  0},
            {"text": "Disagree",          "value": -1},
            {"text": "Strongly disagree", "value": -2},
        ],
    },
]


TYPOLOGY_DESCRIPTIONS = {
    "Progressive":  "You favor active government, expanded social programs, and progressive social policies.",
    "Liberal":      "You lean left on social issues and support a meaningful role for government in the economy.",
    "Moderate":     "You sit near the center, weighing competing priorities case by case rather than by ideology.",
    "Libertarian":  "You favor limited government across both economic and social spheres.",
    "Conservative": "You lean right on economic and social issues and prefer a smaller federal footprint.",
    "Populist":     "You favor government intervention in the economy but lean culturally conservative.",
}


def _typology_for(econ: float, soc: float) -> str:
    """Map an (economic, social) score pair to a six-bucket typology label."""
    if -1.0 <= econ <= 1.0 and -1.0 <= soc <= 1.0:
        return "Moderate"
    if econ < 0 and soc < 0:
        return "Progressive"
    if econ < 0 and soc >= 0:
        return "Populist"
    if econ >= 0 and soc < 0:
        return "Liberal"
    if econ >= 0 and soc >= 0 and econ > 1.5 and soc > 1.5:
        return "Conservative"
    if econ >= 0 and soc >= 0 and econ > soc:
        return "Libertarian"
    return "Conservative"


# ---------------------------------------------------------------------------
# GET /typology/questions
# ---------------------------------------------------------------------------
@router.get("/questions")
def list_questions():
    return {"questions": QUESTIONS, "total": len(QUESTIONS)}


# ---------------------------------------------------------------------------
# POST /typology/submit
# ---------------------------------------------------------------------------
class _Answer(BaseModel):
    question_id: int
    value: float


class TypologySubmitIn(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=120)
    answers: List[_Answer]


@router.post("/submit")
def submit_quiz(payload: TypologySubmitIn, db: Session = Depends(get_db)):
    if not payload.answers:
        raise HTTPException(status_code=400, detail="answers must not be empty")

    by_id = {q["id"]: q for q in QUESTIONS}
    econ_total, econ_n, soc_total, soc_n = 0.0, 0, 0.0, 0
    answers_map: dict[int, float] = {}

    for a in payload.answers:
        q = by_id.get(a.question_id)
        if not q:
            continue
        v = max(-2.0, min(2.0, float(a.value)))
        answers_map[a.question_id] = v
        if q["axis"] == "economic":
            econ_total += v; econ_n += 1
        else:
            soc_total += v; soc_n += 1

    econ_score = econ_total / econ_n if econ_n else 0.0
    soc_score = soc_total / soc_n if soc_n else 0.0
    typology = _typology_for(econ_score, soc_score)
    description = TYPOLOGY_DESCRIPTIONS.get(typology, "")

    scores_payload = {
        "economic": round(econ_score, 3),
        "social":   round(soc_score, 3),
        "answers":  answers_map,
    }

    db.execute(
        text(
            """
            INSERT INTO typology_results
              (anon_user_id, scores, typology, typology_label, economic_score,
               social_score, responses, created_at)
            VALUES
              (:uid, CAST(:scores AS JSONB), :typology, :typology, :econ,
               :soc, :responses, NOW())
            """
        ),
        {
            "uid": payload.user_id[:120],
            "scores": json.dumps(scores_payload),
            "typology": typology,
            "econ": econ_score,
            "soc": soc_score,
            "responses": json.dumps(answers_map),
        },
    )
    db.commit()

    return {
        "typology": typology,
        "description": description,
        "economic_score": round(econ_score, 3),
        "social_score": round(soc_score, 3),
    }
