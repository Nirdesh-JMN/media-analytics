import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]


# ─────────────────────────────────────────────
# Get session profile
# ─────────────────────────────────────────────

async def get_session_profile(conn, session_id):
    rows = await conn.fetch("""
        SELECT 
            AVG(f.runtime_minutes) AS avg_runtime,
            ARRAY_AGG(DISTINCT g.name) AS genres
        FROM session_films sf
        JOIN films f ON sf.film_id = f.id
        LEFT JOIN film_genres fg ON f.id = fg.film_id
        LEFT JOIN genres g ON fg.genre_id = g.id
        WHERE sf.session_id = $1
    """, session_id)

    row = rows[0]

    return {
        "avg_runtime": row["avg_runtime"],
        "genres": row["genres"] or []
    }


# ─────────────────────────────────────────────
# Recommend films
# ─────────────────────────────────────────────

async def recommend_movies(session_id, limit=5):
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        profile = await get_session_profile(conn, session_id)

        rows = await conn.fetch("""
            SELECT 
                f.id,
                f.title,
                f.runtime_minutes,
                COUNT(g.name) FILTER (
                    WHERE g.name = ANY($1)
                ) AS genre_match_score,
                ABS(f.runtime_minutes - $2) AS runtime_diff
            FROM films f
            LEFT JOIN film_genres fg ON f.id = fg.film_id
            LEFT JOIN genres g ON fg.genre_id = g.id
            WHERE f.id NOT IN (
                SELECT film_id FROM session_films WHERE session_id = $3
            )
            GROUP BY f.id
            ORDER BY 
                genre_match_score DESC,
                runtime_diff ASC
            LIMIT $4
        """,
        profile["genres"],
        profile["avg_runtime"],
        session_id,
        limit)

        return rows

    finally:
        await conn.close()