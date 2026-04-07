from fastapi import FastAPI
import asyncpg
import os
from dotenv import load_dotenv

from recommendation_engine import recommend_movies

load_dotenv()

app = FastAPI()

DATABASE_URL = os.environ["DATABASE_URL"]


@app.get("/")
def root():
    return {"message": "Media Analytics API running 🚀"}


@app.get("/recommend/{session_id}")
async def get_recommendations(session_id: int):
    results = await recommend_movies(session_id)

    return [
        {
            "title": r["title"],
            "score": r["genre_match_score"]
        }
        for r in results
    ]