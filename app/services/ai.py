import google.generativeai as genai
from app.config import GOOGLE_AI_API_KEY

genai.configure(api_key=GOOGLE_AI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash")

def classify_typology(survey_responses: dict) -> dict:
    prompt = f"""You are a political typology classifier. Based on the following survey responses, 
classify the user on two scales (0-100):
- economic_score: 0 = far left, 100 = far right
- social_score: 0 = far progressive, 100 = far conservative
Also determine engagement_level: Low, Medium, or High.
And provide a typology_label (e.g., "Moderate Progressive", "Conservative Libertarian").

Survey responses: {survey_responses}

Respond ONLY in this exact JSON format:
{{"economic_score": 0, "social_score": 0, "engagement_level": "", "typology_label": ""}}"""

    response = model.generate_content(prompt)
    return response.text

def summarize_official(official_data: dict) -> str:
    prompt = f"""Provide a brief, unbiased summary of this elected official suitable for 
a civic engagement platform. Include their key policy positions and voting record highlights.
Keep it factual and balanced. 2-3 paragraphs max.

Official info: {official_data}"""

    response = model.generate_content(prompt)
    return response.text

def analyze_sentiment(article_text: str) -> dict:
    prompt = f"""Analyze the political sentiment of this article. 
Rate it on a scale from -1.0 (strongly left-leaning) to 1.0 (strongly right-leaning).
0.0 is perfectly neutral.

Article: {article_text}

Respond ONLY in this exact JSON format:
{{"sentiment_score": 0.0, "lean": "", "confidence": 0.0, "summary": ""}}"""

    response = model.generate_content(prompt)
    return response.text

def recommend_content(typology: dict, available_content: list) -> str:
    prompt = f"""You are a content recommendation engine for a civic engagement platform.
Based on the user's political typology, recommend balanced content that:
1. Includes perspectives they agree with
2. Includes perspectives that challenge their views
3. Prioritizes factual, unbiased sources

User typology: {typology}
Available content: {available_content}

Respond with a ranked list of content IDs and brief explanations."""

    response = model.generate_content(prompt)
    return response.text

def moderate_discussion(message: str) -> dict:
    prompt = f"""You are a discussion moderator for a civic engagement platform.
Analyze this message for:
1. Is it civil and respectful?
2. Does it contain hate speech or personal attacks?
3. Is it on-topic for political discussion?
4. Suggested action: approve, flag, or reject

Message: {message}

Respond ONLY in this exact JSON format:
{{"is_civil": true, "has_hate_speech": false, "is_on_topic": true, "action": "", "reason": ""}}"""

    response = model.generate_content(prompt)
    return response.text