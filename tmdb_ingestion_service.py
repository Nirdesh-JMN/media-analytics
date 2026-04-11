from __future__ import annotations

import asyncio
import logging
import os
import sys
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
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("tmdb.ingestion")


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Configuration
# ─────────────────────────────────────────────────────────────────────────────

TMDB_API_KEY   = os.environ["TMDB_API_KEY"]
TMDB_BASE_URL  = "https://api.themoviedb.org/3"
DATABASE_URL   = os.environ["DATABASE_URL"]

# ── Rate-limit ───────────────────────────────────────────────────────────────
# 38 / 10 s = 5 % below TMDB's hard cap of 40 / 10 s
RATE_LIMIT_REQUESTS = 38
RATE_LIMIT_WINDOW   = 10.0

# ── Concurrency ──────────────────────────────────────────────────────────────
# 5 concurrent movie coroutines x 3 sub-requests = up to 15 live connections.
# Safe on a VPN. Raise to 10 on a direct network connection.
CONCURRENT_FETCHES  = 5

# ── Retry / timeout ──────────────────────────────────────────────────────────
MAX_RETRIES         = 5
RETRY_BASE_DELAY    = 3.0           # seconds — raised for VPN jitter

# Separate connect vs. total timeouts.
# VPN tunnels can take 3-8 s to establish on first connect.
HTTP_TIMEOUT = aiohttp.ClientTimeout(
    total        = 45,   # full request+response window
    connect      = 10,   # TCP connect
    sock_connect = 10,   # socket-level connect
    sock_read    = 30,   # per-chunk read (allows slow VPN tunnels)
)

# ── Database pool ────────────────────────────────────────────────────────────
DB_POOL_MIN = 1
DB_POOL_MAX = 5
DB_TIMEOUT  = 60    # seconds per statement


# ─────────────────────────────────────────────────────────────────────────────
# 2.  Token-Bucket Rate Limiter
# ─────────────────────────────────────────────────────────────────────────────

class TokenBucketLimiter:
    """
    Async Token-Bucket — mathematically correct, non-blocking.

    Formula:
        tokens_refilled = (elapsed_seconds / window) x capacity

    The bucket starts full. Each HTTP call consumes one token. When empty,
    acquire() sleeps for exactly the time needed to refill one token.
    Sleeping happens OUTSIDE the lock so queued coroutines don't serialise.

    Guarantee: <= 38 requests per any rolling 10-second window.
    """

    def __init__(self, capacity: int, window: float) -> None:
        self.capacity     = float(capacity)
        self.window       = window
        self.tokens       = float(capacity)
        self._last_refill = time.monotonic()
        self._lock        = asyncio.Lock()

    def _refill(self) -> None:
        now              = time.monotonic()
        elapsed          = now - self._last_refill
        gained           = elapsed * (self.capacity / self.window)
        self.tokens      = min(self.capacity, self.tokens + gained)
        self._last_refill = now

    async def acquire(self) -> None:
        async with self._lock:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            deficit      = 1.0 - self.tokens
            wait_seconds = deficit / (self.capacity / self.window)

        # Sleep OUTSIDE the lock — other coroutines queue efficiently
        log.debug("Rate limiter: waiting %.3fs for token refill", wait_seconds)
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
    Parsed /configuration response — cached once per process lifetime.
    Builds absolute HTTPS image URLs from the CDN base + size choice.
    """
    image_base_url : str
    poster_size    : str
    backdrop_size  : str
    profile_size   : str

    def poster_url(self, path: str | None) -> str | None:
        return f"{self.image_base_url}{self.poster_size}{path}" if path else None

    def backdrop_url(self, path: str | None) -> str | None:
        return f"{self.image_base_url}{self.backdrop_size}{path}" if path else None

    def profile_url(self, path: str | None) -> str | None:
        return f"{self.image_base_url}{self.profile_size}{path}" if path else None


# ─────────────────────────────────────────────────────────────────────────────
# 4.  HTTP Client Wrapper
# ─────────────────────────────────────────────────────────────────────────────

# Status codes worth retrying (includes 429 handled separately)
_RETRYABLE_STATUSES = {500, 502, 503, 504}

# Network / connection errors that VPNs commonly produce
_RETRYABLE_EXCEPTIONS = (
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientOSError,
    aiohttp.ClientPayloadError,
    asyncio.TimeoutError,
)


@dataclass
class TmdbClient:
    """
    Thin async wrapper around aiohttp.ClientSession.
    Handles: rate-limiting, retries, exponential backoff, 429, VPN errors.
    """
    session : aiohttp.ClientSession
    limiter : TokenBucketLimiter
    _config : TmdbConfig | None = field(default=None, repr=False)

    # ── Core GET with retry + backoff ─────────────────────────────────────

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        url         = f"{TMDB_BASE_URL}{path}"
        base_params = {"api_key": TMDB_API_KEY, "language": "en-US"}
        if params:
            base_params.update(params)

        last_exc: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            # Consume a rate-limit token BEFORE dispatching the request
            await self.limiter.acquire()

            try:
                async with self.session.get(
                    url,
                    params  = base_params,
                    timeout = HTTP_TIMEOUT,
                ) as resp:

                    # ── 429: honour Retry-After header ────────────────────
                    if resp.status == 429:
                        retry_after = float(
                            resp.headers.get("Retry-After", RETRY_BASE_DELAY * attempt)
                        )
                        log.warning(
                            "429 on %-40s  backing off %.1fs  (attempt %d/%d)",
                            path, retry_after, attempt, MAX_RETRIES,
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    # ── 5xx: exponential backoff ──────────────────────────
                    if resp.status in _RETRYABLE_STATUSES:
                        wait = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                        log.warning(
                            "HTTP %d on %-40s  retrying in %.1fs  (attempt %d/%d)",
                            resp.status, path, wait, attempt, MAX_RETRIES,
                        )
                        await asyncio.sleep(wait)
                        continue

                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                    log.debug("GET %-50s  200  (attempt %d)", path, attempt)
                    return data

            except _RETRYABLE_EXCEPTIONS as exc:
                last_exc = exc
                wait = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.warning(
                    "Network error on %-40s  (attempt %d/%d) %s — retry in %.1fs",
                    path, attempt, MAX_RETRIES, type(exc).__name__, wait,
                )
                await asyncio.sleep(wait)

            except Exception as exc:
                # Non-retryable (e.g. JSON decode error) — fail immediately
                log.error("Non-retryable error on %s: %s", path, exc)
                raise

        raise RuntimeError(
            f"All {MAX_RETRIES} attempts failed for {path}"
            + (f": {last_exc}" if last_exc else "")
        )

    # ── /configuration (cached) ───────────────────────────────────────────

    async def get_configuration(self) -> TmdbConfig:
        if self._config:
            return self._config

        log.info("Fetching TMDB /configuration ...")
        data   = await self._get("/configuration")
        images = data["images"]

        poster_sizes   = images.get("poster_sizes",   ["w500"])
        backdrop_sizes = images.get("backdrop_sizes", ["w1280"])
        profile_sizes  = images.get("profile_sizes",  ["w185"])

        self._config = TmdbConfig(
            image_base_url = images["secure_base_url"],
            poster_size    = "w500"  if "w500"  in poster_sizes  else poster_sizes[-2],
            backdrop_size  = "w1280" if "w1280" in backdrop_sizes else backdrop_sizes[-2],
            profile_size   = "w185"  if "w185"  in profile_sizes  else profile_sizes[-1],
        )
        log.info(
            "TMDB CDN: %s  poster=%s  backdrop=%s  profile=%s",
            self._config.image_base_url,
            self._config.poster_size,
            self._config.backdrop_size,
            self._config.profile_size,
        )
        return self._config

    # ── Per-movie endpoint fetchers ───────────────────────────────────────

    async def get_movie_details(self, tmdb_id: int) -> dict:
        return await self._get(f"/movie/{tmdb_id}")

    async def get_movie_credits(self, tmdb_id: int) -> dict:
        return await self._get(f"/movie/{tmdb_id}/credits")

    async def get_movie_keywords(self, tmdb_id: int) -> dict:
        return await self._get(f"/movie/{tmdb_id}/keywords")

    # ── 3-way concurrent fan-out for a single movie ───────────────────────

    async def fetch_full_movie(self, tmdb_id: int) -> dict:
        """
        Fire details + credits + keywords concurrently.

        return_exceptions=True: if one sub-request fails after all retries,
        the other two still complete. We then inspect all three results and
        re-raise the first exception found — no silent partial data.

        VPN note: the three requests share the same keep-alive TCP connection
        to api.themoviedb.org, so only ONE VPN tunnel setup overhead is paid
        per movie regardless of how many sub-requests are made.
        """
        log.info("  -> fetching movie %d  (details + credits + keywords)", tmdb_id)
        t0 = time.monotonic()

        raw = await asyncio.gather(
            self.get_movie_details(tmdb_id),
            self.get_movie_credits(tmdb_id),
            self.get_movie_keywords(tmdb_id),
            return_exceptions=True,
        )

        # Surface the first exception if any sub-request ultimately failed
        for r in raw:
            if isinstance(r, Exception):
                raise r

        details, credits, keywords = raw
        elapsed = time.monotonic() - t0
        log.info(
            "  ok movie %d  fetched in %.2fs  (%s)",
            tmdb_id, elapsed, details.get("title", "unknown"),
        )
        return {"details": details, "credits": credits, "keywords": keywords}


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Data Transformers  (TMDB response -> schema rows)
# ─────────────────────────────────────────────────────────────────────────────

CREW_ROLE_MAP: dict[str, str] = {
    "Director":                "director",
    "Producer":                "producer",
    "Screenplay":              "writer",
    "Writer":                  "writer",
    "Director of Photography": "cinematographer",
    "Editor":                  "editor",
    "Original Music Composer": "composer",
    "Production Design":       "production_design",
    "Visual Effects":          "vfx_supervisor",
}


def transform_film(details: dict, config: TmdbConfig) -> dict:
    release_year: int | None = None
    raw_date = details.get("release_date") or ""
    if len(raw_date) >= 4:
        try:
            release_year = int(raw_date[:4])
        except ValueError:
            pass

    return {
        "tmdb_id":         details["id"],
        "imdb_id":         details.get("imdb_id"),
        "title":           details.get("title", ""),
        "original_title":  details.get("original_title"),
        "release_year":    release_year,
        "runtime_minutes": details.get("runtime"),
        "language_code":   details.get("original_language"),
        "average_rating":  details.get("vote_average"),
        "synopsis":        details.get("overview"),
        "poster_url":      config.poster_url(details.get("poster_path")),
    }


def transform_cast(credits: dict, config: TmdbConfig) -> list[dict]:
    rows = []
    for member in credits.get("cast", [])[:50]:
        rows.append({
            "tmdb_person_id": member["id"],
            "full_name":      member.get("name", ""),
            "profile_url":    config.profile_url(member.get("profile_path")),
            "character_name": member.get("character"),
            "billing_order":  (member.get("order") or 0) + 1,
        })
    return rows


def transform_crew(credits: dict, config: TmdbConfig) -> list[dict]:
    seen: set[tuple[int, str]] = set()
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
    return [
        {"tmdb_genre_id": g["id"], "name": g["name"]}
        for g in details.get("genres", [])
    ]


def transform_keywords(keywords_data: dict) -> list[str]:
    return [kw["name"] for kw in keywords_data.get("keywords", [])]


# ─────────────────────────────────────────────────────────────────────────────
# 6.  Database Writer  (asyncpg — upsert-safe, one transaction per film)
# ─────────────────────────────────────────────────────────────────────────────

async def upsert_movie(
    conn   : asyncpg.Connection,
    payload: dict,
    config : TmdbConfig,
) -> None:
    """
    Write one fully-fetched movie into PostgreSQL atomically.
    INSERT ... ON CONFLICT DO UPDATE makes every run fully idempotent.
    All writes for a single film are in ONE transaction.
    """
    details    = payload["details"]
    credits    = payload["credits"]

    film_row   = transform_film(details, config)
    cast_rows  = transform_cast(credits, config)
    crew_rows  = transform_crew(credits, config)
    genre_rows = transform_genres(details)

    async with conn.transaction():

        # ── Film ──────────────────────────────────────────────────────────
        film_id: int = await conn.fetchval("""
            INSERT INTO films
                (tmdb_id, imdb_id, title, original_title, release_year,
                 runtime_minutes, language_code, average_rating, synopsis, poster_url)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
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
            film_row["tmdb_id"],    film_row["imdb_id"],
            film_row["title"],      film_row["original_title"],
            film_row["release_year"],
            film_row["runtime_minutes"],
            film_row["language_code"],
            film_row["average_rating"],
            film_row["synopsis"],   film_row["poster_url"],
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

    log.info(
        "  db: tmdb_id=%-8d -> internal id=%-6d  %s",
        film_row["tmdb_id"], film_id, film_row["title"],
    )


# ─────────────────────────────────────────────────────────────────────────────
# 7.  Per-Movie Task  (semaphore-bounded)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class IngestionResult:
    tmdb_id : int
    success : bool
    title   : str   = ""
    elapsed : float = 0.0
    error   : str | None = None


async def process_one(
    tmdb_id  : int,
    client   : TmdbClient,
    pool     : asyncpg.Pool,
    semaphore: asyncio.Semaphore,
    config   : TmdbConfig,
) -> IngestionResult:
    """
    One movie — gated by the semaphore so CONCURRENT_FETCHES is respected.

    CRITICAL DESIGN POINT:
    The semaphore wraps the ENTIRE process_one coroutine (fetch + DB write),
    NOT the individual sub-requests inside fetch_full_movie.

    If the semaphore were placed inside fetch_full_movie, all three
    gather() sub-requests would try to acquire a slot already held by their
    own parent coroutine -> deadlock -> infinite hang (the original bug).
    """
    async with semaphore:
        t0 = time.monotonic()
        try:
            payload = await client.fetch_full_movie(tmdb_id)
            async with pool.acquire() as conn:
                await upsert_movie(conn, payload, config)
            title   = payload["details"].get("title", "")
            elapsed = time.monotonic() - t0
            return IngestionResult(
                tmdb_id=tmdb_id, success=True, title=title, elapsed=elapsed
            )

        except Exception as exc:
            elapsed = time.monotonic() - t0
            log.error(
                "FAILED tmdb_id=%d after %.2fs: %s",
                tmdb_id, elapsed, exc, exc_info=True,
            )
            return IngestionResult(
                tmdb_id=tmdb_id, success=False, elapsed=elapsed, error=str(exc)
            )


# ─────────────────────────────────────────────────────────────────────────────
# 8.  Public Entry Point
# ─────────────────────────────────────────────────────────────────────────────

async def ingest_movies(tmdb_ids: list[int]) -> list[IngestionResult]:
    """
    Ingest an arbitrary list of TMDB movie IDs into PostgreSQL.

    Concurrency model
    -----------------
    TokenBucketLimiter  ->  <= 38 HTTP requests per 10-second window
    asyncio.Semaphore   ->  <= CONCURRENT_FETCHES live movie coroutines
    aiohttp keep-alive  ->  all 3 sub-requests per movie reuse the same
                            TCP connection through the VPN tunnel, paying
                            tunnel setup cost only once per movie

    Deadlock-free guarantee
    -----------------------
    Semaphore is on process_one (the whole movie unit), NOT on individual
    HTTP calls inside fetch_full_movie. This means the 3 sub-requests in
    gather() never compete with each other for a slot already held by
    their own parent — the root cause of the original infinite hang.
    """
    if not tmdb_ids:
        log.warning("ingest_movies called with empty list — nothing to do.")
        return []

    total_t0 = time.monotonic()
    log.info("=" * 60)
    log.info("Starting ingestion for %d movie(s) ...", len(tmdb_ids))
    log.info("  CONCURRENT_FETCHES  = %d", CONCURRENT_FETCHES)
    log.info("  RATE_LIMIT          = %d req / %.0fs", RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)
    log.info("  HTTP timeout total  = 45s  read = 30s")
    log.info("  Max retries         = %d  base_delay = %.1fs", MAX_RETRIES, RETRY_BASE_DELAY)
    log.info("=" * 60)

    limiter   = TokenBucketLimiter(RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)
    semaphore = asyncio.Semaphore(CONCURRENT_FETCHES)

    # ── TCPConnector tuned for VPN ────────────────────────────────────────
    # keepalive_timeout=120  : keep tunnels alive between request bursts
    # force_close=False      : reuse connections — critical for VPN perf
    # limit=30               : max open connections across all hosts
    # ttl_dns_cache=600      : cache DNS 10 min (VPN DNS lookups are slow)
    # enable_cleanup_closed  : drop dead connections proactively
    connector = aiohttp.TCPConnector(
        limit                 = 30,
        keepalive_timeout     = 120,
        force_close           = False,
        ttl_dns_cache         = 600,
        enable_cleanup_closed = True,
    )

    async with aiohttp.ClientSession(
        connector       = connector,
        connector_owner = True,
        headers         = {
            "Accept":          "application/json",
            "Accept-Encoding": "gzip, deflate",   # reduces payload size over VPN
            "Connection":      "keep-alive",
        },
    ) as http_session:

        client = TmdbClient(session=http_session, limiter=limiter)

        # ── Fetch /configuration once (inside the session context) ────────
        config = await client.get_configuration()

        # ── Open DB pool (also inside the session context) ────────────────
        pool = await asyncpg.create_pool(
            DATABASE_URL,
            ssl                              = "require",
            min_size                         = DB_POOL_MIN,
            max_size                         = DB_POOL_MAX,
            command_timeout                  = DB_TIMEOUT,
            max_inactive_connection_lifetime = 300,
        )

        try:
            tasks = [
                process_one(tmdb_id, client, pool, semaphore, config)
                for tmdb_id in tmdb_ids
            ]
            results: list[IngestionResult] = await asyncio.gather(*tasks)
        finally:
            await pool.close()

    # ── Summary ───────────────────────────────────────────────────────────
    total_elapsed = time.monotonic() - total_t0
    ok   = [r for r in results if r.success]
    fail = [r for r in results if not r.success]

    log.info("=" * 60)
    log.info("Ingestion complete in %.2fs", total_elapsed)
    log.info("  Succeeded : %d", len(ok))
    log.info("  Failed    : %d", len(fail))
    if ok:
        avg_t = sum(r.elapsed for r in ok) / len(ok)
        log.info("  Avg time/movie : %.2fs", avg_t)
    for r in ok:
        log.info("    [ok] [%d] %-40s  %.2fs", r.tmdb_id, r.title, r.elapsed)
    for r in fail:
        log.error("    [!!] [%d] %s", r.tmdb_id, r.error)
    log.info("=" * 60)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 9.  CLI entry-point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage  : python tmdb_ingestion_service.py <tmdb_id> [<tmdb_id> ...]")
        print("Example: python tmdb_ingestion_service.py 238 550 278 424")
        sys.exit(1)

    ids = [int(x) for x in sys.argv[1:]]
    asyncio.run(ingest_movies(ids))