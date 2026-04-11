import asyncio
import asyncpg
import sys
from dotenv import load_dotenv
import os

from session_analytics import SessionAnalyticsPipeline

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]


# ─────────────────────────────────────────────
# MATCH PIPELINE EXPECTED OBJECT
# ─────────────────────────────────────────────

class FilmInput:
    def __init__(self, tmdb_id, title, poster_url, runtime_minutes, genres, view_order):
        self.tmdb_id = tmdb_id
        self.title = title
        self.poster_url = poster_url
        self.runtime_minutes = runtime_minutes
        self.genres = genres
        self.view_order = view_order
        self.keywords = []  # required by pipeline


# ─────────────────────────────────────────────
# FETCH SESSION DATA (CORRECT QUERY)
# ─────────────────────────────────────────────

async def fetch_session_data(conn, session_id: int):
    rows = await conn.fetch("""
        SELECT 
            f.id,
            f.tmdb_id,
            f.title,
            f.poster_url,
            f.runtime_minutes,
            sf.view_order,
            ARRAY_AGG(g.name) AS genres
        FROM session_films sf
        JOIN films f ON sf.film_id = f.id
        LEFT JOIN film_genres fg ON f.id = fg.film_id
        LEFT JOIN genres g ON fg.genre_id = g.id
        WHERE sf.session_id = $1
        GROUP BY f.id, sf.view_order
        ORDER BY sf.view_order;
    """, session_id)

    films = []

    for r in rows:
        films.append(
            FilmInput(
                tmdb_id=r["tmdb_id"],
                title=r["title"],
                poster_url=r["poster_url"],
                runtime_minutes=r["runtime_minutes"],
                genres=r["genres"] or [],
                view_order=r["view_order"],
            )
        )

    return films


# ─────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────

async def run(session_id: int):
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")

    try:
        films = await fetch_session_data(conn, session_id)

        if not films:
            print("❌ No session data found")
            return

        pipeline = SessionAnalyticsPipeline()

        # THIS IS CORRECT METHOD
        results = pipeline.run(films)

        print("\n🎬 SESSION ANALYTICS\n")

        for r in results:
            print(f"{r.view_order}. {r.title}")
            print("   🎨 Colors:", r.dominant_colors)
            print("   🌈 Mood:", r.color_mood)
            print("   ⚡ Pacing:", r.pacing_score)
            print()

    finally:
        await conn.close()


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analytics_from_db_v2.py <session_id>")
        sys.exit(1)

    session_id = int(sys.argv[1])
    asyncio.run(run(session_id))