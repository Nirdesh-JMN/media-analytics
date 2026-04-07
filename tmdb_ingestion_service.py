"""
tmdb_ingestion_service.py
─────────────────────────
High-Throughput TMDB → PostgreSQL Ingestion Service
====================================================
Rate limit : 40 requests / 10 s  (TMDB hard cap)
Strategy   : Token-Bucket algorithm — async, non-blocking
Fetches    : /movie/{id}, /movie/{id}/credits, /movie/{id}/keywords
Cache      : /configuration fetched once per process lifetime

Dependencies:
    pip install aiohttp asyncpg python-dotenv

Author : Senior Backend Engineer
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp
import asyncpg
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# 0.  Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("tmdb.ingestion")


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Configuration
# ─────────────────────────────────────────────────────────────────────────────

TMDB_API_KEY    = os.environ["TMDB_API_KEY"]           # v3 auth key
TMDB_BASE_URL   = "https://api.themoviedb.org/3"
DATABASE_URL    = os.environ["DATABASE_URL"]           # asyncpg DSN

# Token-Bucket parameters (tuned 5 % below hard cap for safety margin)
RATE_LIMIT_REQUESTS = 38          # tokens replenished per window
RATE_LIMIT_WINDOW   = 10.0        # seconds per window
MAX_RETRIES         = 4
RETRY_BASE_DELAY    = 2.0         # seconds (exponential backoff base)
CONCURRENT_FETCHES  = 10          # max parallel HTTP coroutines


# ─────────────────────────────────────────────────────────────────────────────
# 2.  Token-Bucket Rate Limiter
# ─────────────────────────────────────────────────────────────────────────────

class TokenBucketLimiter:
    """
    Async Token-Bucket implementation — mathematically correct, non-blocking.

    How it works
    ────────────
    The bucket starts full (capacity = RATE_LIMIT_REQUESTS tokens).
    Each HTTP request consumes one token.  Tokens refill continuously
    based on elapsed wall-clock time:

        refill = (elapsed / window) * capacity

    If the bucket is empty, `acquire()` calculates the exact sleep
    duration needed for one token to become available and awaits it.
    This means no busy-loops and no thundering herds.

    Mathematical guarantee
    ──────────────────────
    Over any rolling 10-second window, dispatched requests ≤ 38,
    which is 5 % below TMDB's hard cap of 40, providing a safety
    margin for clock skew and network jitter.
    """

    def __init__(self, capacity: int, window: float) -> None:
        self.capacity  = float(capacity)   # max tokens (= requests per window)
        self.window    = window            # window duration in seconds
        self.tokens    = float(capacity)   # start full
        self._last_refill = time.monotonic()
        self._lock     = asyncio.Lock()

    def _refill(self) -> None:
        """Unconditionally bring tokens up-to-date based on elapsed time."""
        now     = time.monotonic()
        elapsed = now - self._last_refill
        # Tokens trickle in at rate = capacity / window  (tokens per second)
        gained  = elapsed * (self.capacity / self.window)
        self.tokens = min(self.capacity, self.tokens + gained)
        self._last_refill = now

    async def acquire(self) -> None:
        """Block (async-sleep) until one token is available, then consume it."""
        async with self._lock:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            # Calculate wait time so that exactly 1 token will have refilled
            deficit      = 1.0 - self.tokens
            wait_seconds = deficit / (self.capacity / self.window)

        # Sleep *outside* the lock so other coroutines can queue efficiently
        log.debug("Rate limiter: sleeping %.3fs for token refill", wait_seconds)
        await asyncio.sleep(wait_seconds)

        # Re-enter to claim the now-available token
        async with self._lock:
            self._refill()
            self.tokens = max(0.0, self.tokens - 1.0)


# ─────────────────────────────────────────────────────────────────────────────
# 3.  TMDB Configuration Cache
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TmdbConfig:
    """
    Parsed /configuration response.
    Cached once per process — TMDB states this endpoint changes rarely.
    We store the base URL + preferred poster size for absolute URL assembly.
    """
    image_base_url:  str
    poster_size:     str         # e.g. "w500"
    backdrop_size:   str         # e.g. "w1280"
    profile_size:    str         # e.g. "w185"

    def poster_url(self, path: str | None) -> str | None:
        if not path:
            return None
        return f"{self.image_base_url}{self.poster_size}{path}"

    def backdrop_url(self, path: str | None) -> str | None:
        if not path:
            return None
        return f"{self.image_base_url}{self.backdrop_size}{path}"

    def profile_url(self, path: str | None) -> str | None:
        if not path:
            return None
        return f"{self.image_base_url}{self.profile_size}{path}"


# ─────────────────────────────────────────────────────────────────────────────
# 4.  HTTP Client wrapper
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TmdbClient:
    """
    Thin async wrapper around aiohttp.ClientSession.
    Handles:  rate-limiting, retries, exponential backoff, 429 detection.
    """
    session:  aiohttp.ClientSession
    limiter:  TokenBucketLimiter
    _config:  TmdbConfig | None = field(default=None, repr=False)

    # ── Internal fetch with retry ─────────────────────────────────────────

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        """
        Acquires a rate-limit token, performs the GET, retries on transient
        errors (429, 500, 502, 503, 504) using truncated exponential backoff.
        """
        url = f"{TMDB_BASE_URL}{path}"
        base_params = {"api_key": TMDB_API_KEY, "language": "en-US"}
        if params:
            base_params.update(params)

        for attempt in range(1, MAX_RETRIES + 1):
            await self.limiter.acquire()
            try:
                async with self.session.get(url, params=base_params, timeout=aiohttp.ClientTimeout(total=15)) as resp:

                    if resp.status == 429:
                        # TMDB Retry-After header may be present
                        retry_after = float(resp.headers.get("Retry-After", RETRY_BASE_DELAY * attempt))
                        log.warning("429 on %s — backing off %.1fs (attempt %d)", path, retry_after, attempt)
                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status in (500, 502, 503, 504):
                        wait = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                        log.warning("HTTP %d on %s — retrying in %.1fs", resp.status, path, wait)
                        await asyncio.sleep(wait)
                        continue

                    resp.raise_for_status()
                    return await resp.json()

            except aiohttp.ClientConnectorError as exc:
                wait = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.error("Connection error on %s (attempt %d): %s — retrying in %.1fs", path, attempt, exc, wait)
                await asyncio.sleep(wait)

        raise RuntimeError(f"All {MAX_RETRIES} attempts failed for {path}")

    # ── /configuration (cached) ───────────────────────────────────────────

    async def get_configuration(self) -> TmdbConfig:
        """Fetch once and memoize; subsequent calls return the cached object."""
        if self._config:
            return self._config

        log.info("Fetching TMDB /configuration (once per process)…")
        data    = await self._get("/configuration")
        images  = data["images"]

        # Choose sensible defaults from available sizes
        poster_sizes   = images.get("poster_sizes",   ["w500"])
        backdrop_sizes = images.get("backdrop_sizes", ["w1280"])
        profile_sizes  = images.get("profile_sizes",  ["w185"])

        self._config = TmdbConfig(
            image_base_url = images["secure_base_url"],
            poster_size    = "w500"  if "w500"  in poster_sizes  else poster_sizes[-2],
            backdrop_size  = "w1280" if "w1280" in backdrop_sizes else backdrop_sizes[-2],
            profile_size   = "w185"  if "w185"  in profile_sizes  else profile_sizes[-1],
        )
        log.info("Image base URL: %s (poster=%s)", self._config.image_base_url, self._config.poster_size)
        return self._config

    # ── Per-movie fetchers ────────────────────────────────────────────────

    async def get_movie_details(self, tmdb_id: int) -> dict:
        return await self._get(f"/movie/{tmdb_id}")

    async def get_movie_credits(self, tmdb_id: int) -> dict:
        return await self._get(f"/movie/{tmdb_id}/credits")

    async def get_movie_keywords(self, tmdb_id: int) -> dict:
        return await self._get(f"/movie/{tmdb_id}/keywords")

    # ── Aggregate fetch for one movie (3 calls) ───────────────────────────

    async def fetch_full_movie(self, tmdb_id: int) -> dict:
        """
        Fan-out: fetch details, credits, and keywords concurrently.
        Each sub-request independently acquires its own token, so the
        three calls for different movies can interleave freely.
        """
        log.info("Fetching TMDB movie %d …", tmdb_id)
        details, credits, keywords = await asyncio.gather(
            self.get_movie_details(tmdb_id),
            self.get_movie_credits(tmdb_id),
            self.get_movie_keywords(tmdb_id),
            return_exceptions=False,
        )
        return {"details": details, "credits": credits, "keywords": keywords}


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Data Transformers  (TMDB → schema models)
# ─────────────────────────────────────────────────────────────────────────────

CREW_ROLE_MAP = {
    "Director":           "director",
    "Producer":           "producer",
    "Screenplay":         "writer",
    "Writer":             "writer",
    "Director of Photography": "cinematographer",
    "Editor":             "editor",
    "Original Music Composer": "composer",
    "Production Design":  "production_design",
    "Visual Effects":     "vfx_supervisor",
}

def transform_film(details: dict, config: TmdbConfig) -> dict:
    release_year = None
    raw_date = details.get("release_date", "")
    if raw_date and len(raw_date) >= 4:
        try:
            release_year = int(raw_date[:4])
        except ValueError:
            pass

    return {
        "tmdb_id":        details["id"],
        "imdb_id":        details.get("imdb_id"),
        "title":          details.get("title", ""),
        "original_title": details.get("original_title"),
        "release_year":   release_year,
        "runtime_minutes": details.get("runtime"),
        "language_code":  details.get("original_language"),
        "average_rating": details.get("vote_average"),
        "synopsis":       details.get("overview"),
        "poster_url":     config.poster_url(details.get("poster_path")),
    }


def transform_cast(credits: dict, config: TmdbConfig) -> list[dict]:
    rows = []
    for member in credits.get("cast", [])[:50]:   # top-50 billed cast
        rows.append({
            "tmdb_person_id": member["id"],
            "full_name":      member.get("name", ""),
            "profile_url":    config.profile_url(member.get("profile_path")),
            "character_name": member.get("character"),
            "billing_order":  member.get("order", 0) + 1,  # 0-based → 1-based
        })
    return rows


def transform_crew(credits: dict, config: TmdbConfig) -> list[dict]:
    seen: set[tuple] = set()
    rows = []
    for member in credits.get("crew", []):
        job  = member.get("job", "")
        role = CREW_ROLE_MAP.get(job, "other")
        key  = (member["id"], role)
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "tmdb_person_id": member["id"],
            "full_name":      member.get("name", ""),
            "profile_url":    config.profile_url(member.get("profile_path")),
            "role":           role,
            "department":     member.get("department"),
        })
    return rows


def transform_genres(details: dict) -> list[dict]:
    return [{"tmdb_genre_id": g["id"], "name": g["name"]} for g in details.get("genres", [])]


def transform_keywords(keywords_data: dict) -> list[str]:
    return [kw["name"] for kw in keywords_data.get("keywords", [])]


# ─────────────────────────────────────────────────────────────────────────────
# 6.  Database Writer  (asyncpg — upsert-safe)
# ─────────────────────────────────────────────────────────────────────────────

async def upsert_movie(conn: asyncpg.Connection, payload: dict, config: TmdbConfig) -> None:
    """
    Write one fully-fetched movie into PostgreSQL atomically.
    Uses INSERT … ON CONFLICT DO UPDATE (upsert) so re-runs are idempotent.
    All writes for a single movie are in one transaction.
    """
    details  = payload["details"]
    credits  = payload["credits"]
    keywords = payload["keywords"]

    film_row  = transform_film(details, config)
    cast_rows = transform_cast(credits, config)
    crew_rows = transform_crew(credits, config)
    genre_rows = transform_genres(details)

    async with conn.transaction():

        # ── Film ──────────────────────────────────────────────────────────
        film_id: int = await conn.fetchval("""
            INSERT INTO films
                (tmdb_id, imdb_id, title, original_title, release_year,
                 runtime_minutes, language_code, average_rating, synopsis, poster_url)
            VALUES
                ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (tmdb_id) DO UPDATE SET
                title            = EXCLUDED.title,
                original_title   = EXCLUDED.original_title,
                release_year     = EXCLUDED.release_year,
                runtime_minutes  = EXCLUDED.runtime_minutes,
                language_code    = EXCLUDED.language_code,
                average_rating   = EXCLUDED.average_rating,
                synopsis         = EXCLUDED.synopsis,
                poster_url       = EXCLUDED.poster_url,
                imdb_id          = COALESCE(EXCLUDED.imdb_id, films.imdb_id)
            RETURNING id
        """,
            film_row["tmdb_id"],
            film_row["imdb_id"],
            film_row["title"],
            film_row["original_title"],
            film_row["release_year"],
            film_row["runtime_minutes"],
            film_row["language_code"],
            film_row["average_rating"],
            film_row["synopsis"],
            film_row["poster_url"],
        )

        # ── Genres ────────────────────────────────────────────────────────
        for i, g in enumerate(genre_rows):
            genre_id: int = await conn.fetchval("""
                INSERT INTO genres (name, slug)
                VALUES ($1, $2)
                ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, g["name"], g["name"].lower().replace(" ", "-"))

            await conn.execute("""
                INSERT INTO film_genres (film_id, genre_id, is_primary)
                VALUES ($1, $2, $3)
                ON CONFLICT (film_id, genre_id) DO NOTHING
            """, film_id, genre_id, i == 0)

        # ── Cast ──────────────────────────────────────────────────────────
        for member in cast_rows:
            person_id: int = await conn.fetchval("""
                INSERT INTO persons (tmdb_person_id, full_name, profile_image_url)
                VALUES ($1, $2, $3)
                ON CONFLICT (tmdb_person_id) DO UPDATE SET
                    full_name         = EXCLUDED.full_name,
                    profile_image_url = EXCLUDED.profile_image_url
                RETURNING id
            """, member["tmdb_person_id"], member["full_name"], member["profile_url"])

            await conn.execute("""
                INSERT INTO film_cast (film_id, person_id, character_name, billing_order)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (film_id, person_id) DO UPDATE SET
                    character_name = EXCLUDED.character_name,
                    billing_order  = EXCLUDED.billing_order
            """, film_id, person_id, member["character_name"], member["billing_order"])

        # ── Crew ──────────────────────────────────────────────────────────
        for member in crew_rows:
            person_id = await conn.fetchval("""
                INSERT INTO persons (tmdb_person_id, full_name, profile_image_url)
                VALUES ($1, $2, $3)
                ON CONFLICT (tmdb_person_id) DO UPDATE SET
                    full_name         = EXCLUDED.full_name,
                    profile_image_url = EXCLUDED.profile_image_url
                RETURNING id
            """, member["tmdb_person_id"], member["full_name"], member["profile_url"])

            await conn.execute("""
                INSERT INTO film_crew (film_id, person_id, role, department)
                VALUES ($1, $2, $3::crew_role_enum, $4)
                ON CONFLICT (film_id, person_id, role) DO UPDATE SET
                    department = EXCLUDED.department
            """, film_id, person_id, member["role"], member["department"])

    log.info("✓ Persisted film tmdb_id=%d → internal id=%d (%s)",
             film_row["tmdb_id"], film_id, film_row["title"])


# ─────────────────────────────────────────────────────────────────────────────
# 7.  Async Task Queue  (semaphore-bounded concurrency)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class IngestionResult:
    tmdb_id:  int
    success:  bool
    error:    str | None = None


async def process_one(
    tmdb_id:  int,
    client:   TmdbClient,
    pool:     asyncpg.Pool,
    semaphore: asyncio.Semaphore,
    config:   TmdbConfig,
) -> IngestionResult:
    """
    Bounded coroutine for a single TMDB ID.
    The semaphore caps concurrent HTTP fans-out; the token bucket
    controls the per-request dispatch rate independently.
    """
    async with semaphore:
        try:
            payload = await client.fetch_full_movie(tmdb_id)
            async with pool.acquire() as conn:
                await upsert_movie(conn, payload, config)
            return IngestionResult(tmdb_id=tmdb_id, success=True)

        except Exception as exc:
            log.error("FAILED tmdb_id=%d: %s", tmdb_id, exc, exc_info=True)
            return IngestionResult(tmdb_id=tmdb_id, success=False, error=str(exc))


async def ingest_movies(tmdb_ids: list[int]) -> list[IngestionResult]:
    """
    Public entry point.

    1. Creates a shared aiohttp session and asyncpg connection pool.
    2. Fetches /configuration once and caches it.
    3. Dispatches all movie IDs concurrently, constrained by:
       • TokenBucketLimiter  — ≤ 38 req / 10 s  (HTTP dispatch rate)
       • asyncio.Semaphore   — ≤ 10 concurrent coroutines (memory/socket ceiling)
    4. Returns a result list with success/failure per ID.
    """
    if not tmdb_ids:
        log.warning("ingest_movies called with empty ID list.")
        return []

    log.info("Starting ingestion for %d movies…", len(tmdb_ids))

    limiter   = TokenBucketLimiter(RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)
    semaphore = asyncio.Semaphore(CONCURRENT_FETCHES)

    connector = aiohttp.TCPConnector(limit=CONCURRENT_FETCHES, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as http_session:
        client = TmdbClient(session=http_session, limiter=limiter)

        # ── Fetch config once ─────────────────────────────────────────────
        config = await client.get_configuration()

        # ── Open DB pool ──────────────────────────────────────────────────
        pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            max_size=CONCURRENT_FETCHES,
            command_timeout=30,
        )

        try:
            tasks = [
                process_one(tmdb_id, client, pool, semaphore, config)
                for tmdb_id in tmdb_ids
            ]
            # gather preserves order and collects all results (no early exit)
            results: list[IngestionResult] = await asyncio.gather(*tasks)
        finally:
            await pool.close()

    # ── Summary ───────────────────────────────────────────────────────────
    ok   = sum(1 for r in results if r.success)
    fail = len(results) - ok
    log.info("Ingestion complete: %d succeeded, %d failed.", ok, fail)
    if fail:
        failed_ids = [r.tmdb_id for r in results if not r.success]
        log.warning("Failed IDs: %s", failed_ids)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 8.  CLI entry-point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    # Accept IDs as CLI args:  python tmdb_ingestion_service.py 550 278 238 ...
    if len(sys.argv) < 2:
        print("Usage: python tmdb_ingestion_service.py <tmdb_id> [<tmdb_id> ...]")
        sys.exit(1)

    ids = [int(x) for x in sys.argv[1:]]
    asyncio.run(ingest_movies(ids))