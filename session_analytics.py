"""
session_analytics.py
────────────────────
Media Analytics Engine — Visual & Pacing Metrics
=================================================
Algorithms
  1. ColorPaletteAnalyzer  — K-Means dominant color extraction from poster URLs
  2. PacingScorer          — Composite pacing heuristic (runtime + keyword signals)
  3. SessionAnalyticsPipeline — Orchestrates both, returns chronologically sorted output

Dependencies:
    pip install scikit-learn Pillow requests numpy

Author : Senior Data Scientist & Analytics Engineer
"""

from __future__ import annotations

import io
import logging
import re
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional
import colorsys

import numpy as np
import requests
from PIL import Image
from sklearn.cluster import KMeans
from sklearn.exceptions import ConvergenceWarning
import warnings

log = logging.getLogger("media.analytics")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
)


# ═════════════════════════════════════════════════════════════════════════════
# 0.  Shared Data Contracts
# ═════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class HexColor:
    """A validated hex color with optional perceptual metadata."""
    hex_code: str          # e.g. "#3A7BD5"
    rgb: tuple[int,int,int]
    luminance: float       # 0.0 (black) → 1.0 (white), perceptual (WCAG formula)
    saturation: float      # 0.0 (grey) → 1.0 (vivid)
    pixel_share: float     # fraction of image pixels this cluster owns

    @classmethod
    def from_rgb(cls, r: int, g: int, b: int, pixel_share: float = 0.0) -> "HexColor":
        hex_code = f"#{r:02X}{g:02X}{b:02X}"
        # WCAG relative luminance
        def _linearize(c: int) -> float:
            v = c / 255.0
            return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
        lr, lg, lb = _linearize(r), _linearize(g), _linearize(b)
        luminance  = 0.2126 * lr + 0.7152 * lg + 0.0722 * lb
        # HSV saturation
        _, saturation, _ = colorsys.rgb_to_hsv(r/255, g/255, b/255)
        return cls(hex_code=hex_code, rgb=(r,g,b),
                   luminance=luminance, saturation=saturation,
                   pixel_share=pixel_share)

    def __str__(self) -> str:
        return self.hex_code


@dataclass
class FilmAnalyticsInput:
    """
    One film's worth of data as it arrives from the ingestion pipeline.
    Mirrors the fields available from the schema (Films + SessionFilm.view_order).
    """
    tmdb_id:         int
    title:           str
    view_order:      int               # 1-based position in the watch session
    poster_url:      Optional[str]
    runtime_minutes: Optional[int]
    genres:          list[str]         # primary genre name(s)
    keywords:        list[str]         # TMDB keyword names


@dataclass
class FilmAnalyticsResult:
    """Enriched output, one per film, in view_order sequence."""
    tmdb_id:         int
    title:           str
    view_order:      int
    dominant_colors: list[HexColor]    # top-3 colors, sorted by pixel_share desc
    pacing_score:    float             # 0.0–10.0 normalised
    pacing_signals:  dict              # audit trail for the score components
    color_mood:      str               # derived label from palette analysis


# ═════════════════════════════════════════════════════════════════════════════
# 1.  Color Palette Analyzer
# ═════════════════════════════════════════════════════════════════════════════

# Resize target before clustering — reduces compute without hurting accuracy
_SAMPLE_SIZE = (150, 150)
_N_COLORS    = 3
_KMEANS_INIT = "k-means++"
_KMEANS_ITER = 300
_HTTP_TIMEOUT = 10          # seconds


class ColorPaletteAnalyzer:
    """
    Extracts the top-N dominant colors from a poster URL using K-Means clustering.

    Why K-Means over a simple histogram?
    ──────────────────────────────────────
    A histogram counts every distinct RGB triplet independently. K-Means groups
    perceptually similar pixels into `k` clusters, so "navy blue" and "dark teal"
    collapse into one representative centroid rather than splitting the vote.
    The centroid coordinates are then rounded to the nearest integer to produce
    a clean representative hex code.

    Pixel-share weighting
    ─────────────────────
    Each cluster's `pixel_share` = (cluster population / total pixels). This
    lets downstream code distinguish a dominant background color (share 0.65)
    from an accent color (share 0.08), enabling richer UI decisions.
    """

    def __init__(self, n_colors: int = _N_COLORS, sample_size: tuple = _SAMPLE_SIZE) -> None:
        self.n_colors   = n_colors
        self.sample_size = sample_size

    # ── Image fetching ────────────────────────────────────────────────────

    def _fetch_image(self, url: str) -> Image.Image:
        """Download and decode a poster image; convert to RGB unconditionally."""
        resp = requests.get(url, timeout=_HTTP_TIMEOUT, stream=True)
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content)).convert("RGB")
        return img

    def _prepare_pixel_array(self, img: Image.Image) -> np.ndarray:
        """
        Downsample and flatten the image into a 2-D array of shape (N_pixels, 3).
        Downsampling with LANCZOS reduces N by ~factor 10 while preserving colour
        distribution — K-Means runtime drops from O(n²) to manageable.
        """
        img_small = img.resize(self.sample_size, Image.LANCZOS)
        pixels    = np.array(img_small, dtype=np.float32)        # (H, W, 3)
        return pixels.reshape(-1, 3)                             # (H*W, 3)

    # ── Core clustering ───────────────────────────────────────────────────

    def extract_palette(self, url: str) -> list[HexColor]:
        """
        Main entry point for a single poster URL.
        Returns top-N HexColor objects sorted by pixel_share descending.
        """
        if not url:
            log.warning("Empty poster URL; returning neutral palette.")
            return [HexColor.from_rgb(128, 128, 128, 1.0 / self.n_colors)] * self.n_colors

        try:
            img    = self._fetch_image(url)
            pixels = self._prepare_pixel_array(img)

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", ConvergenceWarning)
                km = KMeans(
                    n_clusters  = self.n_colors,
                    init        = _KMEANS_INIT,
                    max_iter    = _KMEANS_ITER,
                    n_init      = 10,           # multiple restarts → stable centroids
                    random_state= 42,
                )
                km.fit(pixels)

            # Cluster centres are float32; clip to [0,255] then round to int
            centers    = np.clip(km.cluster_centers_, 0, 255).astype(int)
            labels     = km.labels_
            n_pixels   = len(labels)

            colors: list[HexColor] = []
            for idx, center in enumerate(centers):
                share = (labels == idx).sum() / n_pixels
                colors.append(HexColor.from_rgb(int(center[0]), int(center[1]), int(center[2]), share))

            # Sort by pixel share descending (dominant → accent)
            colors.sort(key=lambda c: c.pixel_share, reverse=True)
            log.info("Palette for %s: %s", url[-40:], [str(c) for c in colors])
            return colors

        except Exception as exc:
            log.error("Color extraction failed for %s: %s", url, exc)
            return [HexColor.from_rgb(128, 128, 128, 1.0 / self.n_colors)] * self.n_colors

    def extract_palettes_batch(
        self, url_map: dict[int, Optional[str]], max_workers: int = 8
    ) -> dict[int, list[HexColor]]:
        """
        Process a dict of {tmdb_id: poster_url} concurrently using a thread pool.
        Network I/O is the bottleneck — threads overcome the GIL here since
        the work is not CPU-bound Python but waiting on HTTP responses.
        Returns {tmdb_id: [HexColor, ...]}
        """
        results: dict[int, list[HexColor]] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_id = {
                pool.submit(self.extract_palette, url or ""): tmdb_id
                for tmdb_id, url in url_map.items()
            }
            for future in as_completed(future_to_id):
                tmdb_id = future_to_id[future]
                try:
                    results[tmdb_id] = future.result()
                except Exception as exc:
                    log.error("Palette batch failed tmdb_id=%d: %s", tmdb_id, exc)
                    results[tmdb_id] = [HexColor.from_rgb(128,128,128)] * self.n_colors

        return results

    @staticmethod
    def infer_color_mood(palette: list[HexColor]) -> str:
        """
        Derive a human-readable mood label from a palette.
        Uses the dominant colour's luminance and saturation.

        Mood matrix:
          High lum + high sat → "Vibrant"
          High lum + low  sat → "Airy"
          Low  lum + high sat → "Intense"
          Low  lum + low  sat → "Brooding"
          Mid  lum, any sat   → "Balanced"
        """
        if not palette:
            return "Unknown"
        dominant = palette[0]
        lum, sat = dominant.luminance, dominant.saturation
        # WCAG luminance is perceptual — pure red (#FF0000) has lum ≈ 0.21,
        # pure green ≈ 0.72, white = 1.0. We bucket on saturation first.
        if sat > 0.55 and lum > 0.15:
            return "Vibrant"    # vivid mid-to-bright hues (red, orange, yellow, cyan…)
        elif sat > 0.55 and lum <= 0.15:
            return "Intense"    # vivid but very dark hues
        elif lum > 0.6 and sat <= 0.55:
            return "Airy"       # light / pastel / near-white
        elif lum <= 0.15 and sat <= 0.55:
            return "Brooding"   # dark, desaturated (near-black)
        else:
            return "Balanced"   # mid-tones, moderate saturation


# ═════════════════════════════════════════════════════════════════════════════
# 2.  Pacing Scorer
# ═════════════════════════════════════════════════════════════════════════════

# ── Keyword lexicons ─────────────────────────────────────────────────────────

# fmt: off
HIGH_VELOCITY_KEYWORDS: dict[str, float] = {
    # Action / movement
    "action":           +1.5,  "chase":            +2.0,  "heist":           +1.5,
    "fight":            +1.5,  "battle":           +1.5,  "war":             +1.2,
    "race":             +2.0,  "pursuit":          +1.8,  "escape":          +1.8,
    "fast-paced":       +2.5,  "fast paced":       +2.5,  "high-octane":     +2.5,
    "adrenaline":       +2.0,  "thriller":         +1.0,  "action-packed":   +2.0,
    "explosion":        +1.5,  "gunfight":         +1.8,  "shootout":        +1.8,
    "car chase":        +2.5,  "time pressure":    +2.0,  "ticking clock":   +2.5,
    "non-stop":         +2.5,  "relentless":       +2.0,  "urgency":         +2.0,
    "sprint":           +2.0,  "frantic":          +2.0,  "breakneck":       +2.5,
    "high stakes":      +1.8,  "countdown":        +2.0,  "suspense":        +1.0,
    # Editing & structure signals
    "montage":          +1.5,  "quick cuts":       +2.0,  "fast editing":    +2.0,
    "snappy dialogue":  +1.5,  "rapid fire":       +2.0,
}

LOW_VELOCITY_KEYWORDS: dict[str, float] = {
    # Contemplative / deliberate
    "slow burn":        -2.5,  "slow-burn":        -2.5,  "meditative":      -2.5,
    "contemplative":    -2.0,  "introspective":    -2.0,  "philosophical":   -1.5,
    "reflective":       -1.5,  "pensive":          -1.5,  "quiet":           -1.0,
    "languid":          -2.0,  "deliberate":       -1.5,  "measured":        -1.5,
    "melancholic":      -1.2,  "brooding":         -1.2,  "art house":       -1.5,
    "arthouse":         -1.5,  "slow cinema":      -2.5,  "minimalist":      -2.0,
    "long take":        -2.0,  "silent":           -1.5,  "stillness":       -2.0,
    "patience":         -1.5,  "atmospheric":      -1.0,  "dreamlike":       -1.5,
    "surreal":          -1.0,  "existential":      -1.5,  "mediation":       -1.5,
    "character study":  -1.2,  "slice of life":    -1.5,  "quiet drama":     -2.0,
    "mournful":         -1.5,  "elegy":            -2.0,  "poetic":          -1.5,
}
# fmt: on

# Genre average runtimes (minutes) — derived from TMDB population statistics.
# Used as the baseline for the runtime-pacing component.
GENRE_AVERAGE_RUNTIMES: dict[str, float] = {
    "Action":        112.0,
    "Adventure":     115.0,
    "Animation":      95.0,
    "Comedy":         98.0,
    "Crime":         115.0,
    "Documentary":    94.0,
    "Drama":         117.0,
    "Family":         98.0,
    "Fantasy":       113.0,
    "History":       128.0,
    "Horror":         95.0,
    "Music":         105.0,
    "Mystery":       105.0,
    "Romance":       102.0,
    "Science Fiction":116.0,
    "Thriller":      108.0,
    "TV Movie":       95.0,
    "War":           128.0,
    "Western":       115.0,
}
_GLOBAL_AVERAGE_RUNTIME = 108.0   # fallback when genre is unknown


class PacingScorer:
    """
    Computes a normalised Pacing Score ∈ [0.0, 10.0] for a single film.

    Score Components
    ────────────────
    BASE_SCORE       5.0    — neutral midpoint
    runtime_delta    ±2.5   — capped contribution from runtime deviation
    keyword_delta    ±3.0   — capped contribution from keyword lexicon
    ─────────────────────────────────────────────────────────────────
    raw_score        [−0.5, 10.5]   then clamp to [0.0, 10.0]

    Runtime component
    ─────────────────
    We compute the z-score of the film's runtime relative to its genre:
        z = (genre_avg - runtime) / genre_avg
    A shorter film → positive z (more pace); longer → negative z.
    The z-score is then scaled by a sensitivity constant (2.5) and clamped
    to [−2.5, +2.5] so no single outlier dominates the final score.

    Keyword component
    ─────────────────
    Keyword matching is normalised:
        raw_kw_delta = Σ weight(kw) for each matched keyword
        keyword_delta = clamp(raw_kw_delta, −3.0, +3.0)
    Matching is case-insensitive substring search so "fast-paced action film"
    matches both "fast-paced" and "action".
    """

    BASE_SCORE         = 5.0
    RUNTIME_SENSITIVITY = 2.5    # max ± contribution from runtime
    KEYWORD_CAP        = 3.0     # max ± contribution from keywords

    def __init__(
        self,
        genre_runtimes: dict[str,float] | None = None,
        high_kw: dict[str,float]        | None = None,
        low_kw:  dict[str,float]        | None = None,
    ) -> None:
        self.genre_runtimes = genre_runtimes or GENRE_AVERAGE_RUNTIMES
        self.high_kw        = high_kw        or HIGH_VELOCITY_KEYWORDS
        self.low_kw         = low_kw         or LOW_VELOCITY_KEYWORDS

    # ── Runtime component ─────────────────────────────────────────────────

    def _runtime_delta(
        self, runtime: Optional[int], genres: list[str]
    ) -> tuple[float, dict]:
        """
        Returns (delta, audit_dict).
        delta > 0 → film is shorter than genre avg → contributes positively to pace.
        """
        if runtime is None:
            return 0.0, {"runtime_delta": 0.0, "reason": "runtime_missing"}

        # Use the first known genre's average; fallback to global mean
        genre_avg = _GLOBAL_AVERAGE_RUNTIME
        matched_genre = "global"
        for g in genres:
            if g in self.genre_runtimes:
                genre_avg = self.genre_runtimes[g]
                matched_genre = g
                break

        # z-score relative to genre: positive when shorter than average
        z = (genre_avg - runtime) / genre_avg
        delta = float(np.clip(z * self.RUNTIME_SENSITIVITY, -self.RUNTIME_SENSITIVITY, self.RUNTIME_SENSITIVITY))

        audit = {
            "runtime_minutes": runtime,
            "genre_avg_minutes": genre_avg,
            "matched_genre": matched_genre,
            "z_score": round(z, 4),
            "runtime_delta": round(delta, 4),
        }
        return delta, audit

    # ── Keyword component ─────────────────────────────────────────────────

    def _keyword_delta(self, keywords: list[str]) -> tuple[float, dict]:
        """
        Returns (delta, audit_dict).
        Matches each keyword from the film against both lexicons.
        Uses substring matching so multi-word keywords and partial matches work.
        """
        kw_lower   = [k.lower() for k in keywords]
        matched_hi : list[tuple[str, float]] = []
        matched_lo : list[tuple[str, float]] = []

        for kw in kw_lower:
            for lexeme, weight in self.high_kw.items():
                if lexeme in kw or kw in lexeme:
                    matched_hi.append((lexeme, weight))
            for lexeme, weight in self.low_kw.items():
                if lexeme in kw or kw in lexeme:
                    matched_lo.append((lexeme, weight))

        raw_delta = sum(w for _, w in matched_hi) + sum(w for _, w in matched_lo)
        delta     = float(np.clip(raw_delta, -self.KEYWORD_CAP, self.KEYWORD_CAP))

        audit = {
            "high_velocity_hits": matched_hi,
            "low_velocity_hits":  matched_lo,
            "raw_keyword_delta":  round(raw_delta, 4),
            "keyword_delta":      round(delta, 4),
        }
        return delta, audit

    # ── Composite score ───────────────────────────────────────────────────

    def score(
        self,
        runtime:  Optional[int],
        genres:   list[str],
        keywords: list[str],
    ) -> tuple[float, dict]:
        """
        Compute the pacing score and return (score, full_audit_dict).

        Formula:
            raw = BASE(5.0) + runtime_delta + keyword_delta
            score = clamp(raw, 0.0, 10.0)
        """
        rt_delta,  rt_audit  = self._runtime_delta(runtime, genres)
        kw_delta,  kw_audit  = self._keyword_delta(keywords)

        raw   = self.BASE_SCORE + rt_delta + kw_delta
        final = float(np.clip(raw, 0.0, 10.0))

        signals = {
            "base_score":    self.BASE_SCORE,
            "runtime":       rt_audit,
            "keywords":      kw_audit,
            "raw_score":     round(raw,   4),
            "pacing_score":  round(final, 4),
            "label":         self._label(final),
        }
        return round(final, 2), signals

    @staticmethod
    def _label(score: float) -> str:
        if score >= 8.0: return "High Velocity"
        if score >= 6.0: return "Brisk"
        if score >= 4.0: return "Moderate"
        if score >= 2.0: return "Deliberate"
        return "Glacial"


# ═════════════════════════════════════════════════════════════════════════════
# 3.  Session Analytics Pipeline
# ═════════════════════════════════════════════════════════════════════════════

class SessionAnalyticsPipeline:
    """
    Orchestrates ColorPaletteAnalyzer and PacingScorer over an entire
    watch session's film array, returning results sorted by view_order.

    Processing steps
    ─────────────────
    1. Validate & sort inputs by view_order (chronological).
    2. Batch-fetch all poster images concurrently (thread pool).
    3. Compute pacing scores independently (pure CPU, no I/O).
    4. Assemble FilmAnalyticsResult objects and return sorted list.
    """

    def __init__(
        self,
        color_analyzer: ColorPaletteAnalyzer | None = None,
        pacing_scorer:  PacingScorer         | None = None,
        max_image_workers: int = 8,
    ) -> None:
        self.color_analyzer    = color_analyzer or ColorPaletteAnalyzer()
        self.pacing_scorer     = pacing_scorer  or PacingScorer()
        self.max_image_workers = max_image_workers

    def run(self, films: list[FilmAnalyticsInput]) -> list[FilmAnalyticsResult]:
        """
        Main entry point.

        Parameters
        ──────────
        films : list of FilmAnalyticsInput — may arrive in any order.

        Returns
        ───────
        list of FilmAnalyticsResult sorted ascending by view_order.
        """
        if not films:
            log.warning("Pipeline received empty film list.")
            return []

        # Step 1 — Sort chronologically so session flow is preserved
        films_sorted = sorted(films, key=lambda f: f.view_order)

        log.info(
            "Pipeline: processing %d films (view_order %d → %d)",
            len(films_sorted),
            films_sorted[0].view_order,
            films_sorted[-1].view_order,
        )

        # Step 2 — Batch color extraction (concurrent HTTP)
        url_map = {f.tmdb_id: f.poster_url for f in films_sorted}
        palettes: dict[int, list[HexColor]] = self.color_analyzer.extract_palettes_batch(
            url_map, max_workers=self.max_image_workers
        )

        # Step 3 — Pacing scores (pure computation, single-threaded is fine)
        results: list[FilmAnalyticsResult] = []
        for film in films_sorted:
            palette      = palettes.get(film.tmdb_id, [])
            color_mood   = ColorPaletteAnalyzer.infer_color_mood(palette)
            pacing, sigs = self.pacing_scorer.score(
                runtime  = film.runtime_minutes,
                genres   = film.genres,
                keywords = film.keywords,
            )
            results.append(FilmAnalyticsResult(
                tmdb_id        = film.tmdb_id,
                title          = film.title,
                view_order     = film.view_order,
                dominant_colors= palette,
                pacing_score   = pacing,
                pacing_signals = sigs,
                color_mood     = color_mood,
            ))

        return results   # already in view_order sequence

    # ── Convenience: session-level aggregate stats ─────────────────────────

    @staticmethod
    def session_summary(results: list[FilmAnalyticsResult]) -> dict:
        """
        Compute aggregate analytics across the full session:
        - average / stddev pacing score
        - mood distribution
        - pace arc (how pacing evolved across the session)
        """
        if not results:
            return {}
        scores  = [r.pacing_score for r in results]
        moods   = [r.color_mood   for r in results]
        mood_freq: dict[str, int] = {}
        for m in moods:
            mood_freq[m] = mood_freq.get(m, 0) + 1

        # Pace arc: difference between consecutive pacing scores
        arc = [
            round(results[i].pacing_score - results[i-1].pacing_score, 2)
            for i in range(1, len(results))
        ]
        return {
            "total_films":          len(results),
            "avg_pacing_score":     round(statistics.mean(scores), 2),
            "stddev_pacing_score":  round(statistics.stdev(scores) if len(scores) > 1 else 0.0, 2),
            "min_pacing_score":     min(scores),
            "max_pacing_score":     max(scores),
            "mood_distribution":    mood_freq,
            "pace_arc":             arc,
            "session_energy":       "Escalating" if sum(arc) > 1 else "De-escalating" if sum(arc) < -1 else "Steady",
        }