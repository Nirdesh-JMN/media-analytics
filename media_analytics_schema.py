"""
media_analytics_schema.py
─────────────────────────
High-Throughput Media Analytics Pipeline — PostgreSQL Schema
Designed with SQLAlchemy 2.x (Declarative), strict FK constraints,
and B-Tree indexes tuned for complex self-join overlap queries.

Author : Senior Database Architect
Engine : PostgreSQL 15+
ORM    : SQLAlchemy 2.x
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)

import enum


# ─────────────────────────────────────────────
# 0.  Base & Enumerations
# ─────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class GenderEnum(str, enum.Enum):
    male        = "male"
    female      = "female"
    non_binary  = "non_binary"
    undisclosed = "undisclosed"


class CrewRoleEnum(str, enum.Enum):
    director         = "director"
    producer         = "producer"
    writer           = "writer"
    cinematographer  = "cinematographer"
    editor           = "editor"
    composer         = "composer"
    production_design = "production_design"
    vfx_supervisor   = "vfx_supervisor"
    other            = "other"


class WatchDeviceEnum(str, enum.Enum):
    smart_tv  = "smart_tv"
    mobile    = "mobile"
    tablet    = "tablet"
    desktop   = "desktop"
    console   = "console"


# ─────────────────────────────────────────────
# 1.  PRIMARY TABLES
# ─────────────────────────────────────────────

class User(Base):
    """
    Registered platform users.
    Soft-delete via `is_active`; no hard deletes to preserve analytics history.
    """
    __tablename__ = "users"

    id: Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    username: Mapped[str]         = mapped_column(String(80),  nullable=False, unique=True)
    email: Mapped[str]            = mapped_column(String(255), nullable=False, unique=True)
    display_name: Mapped[Optional[str]] = mapped_column(String(120))
    gender: Mapped[Optional[GenderEnum]] = mapped_column(
        Enum(GenderEnum, name="gender_enum"), nullable=True
    )
    country_code: Mapped[Optional[str]] = mapped_column(String(3))          # ISO-3166-1 alpha-3
    is_active: Mapped[bool]       = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime]  = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime]  = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    sessions: Mapped[List["WatchSession"]] = relationship(back_populates="user")

    __table_args__ = (
        # Partial index: only index active users (dramatically smaller for auth lookups)
        Index("ix_users_email_active", "email", postgresql_where="is_active = TRUE"),
        Index("ix_users_country",      "country_code"),
    )


# ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──

class Genre(Base):
    """
    Canonical genre taxonomy (e.g. Action, Drama, Sci-Fi).
    Kept intentionally small; join via Film_Genres.
    """
    __tablename__ = "genres"

    id: Mapped[int]          = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    name: Mapped[str]        = mapped_column(String(60), nullable=False, unique=True)
    slug: Mapped[str]        = mapped_column(String(60), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Relationships
    film_genres: Mapped[List["FilmGenre"]] = relationship(back_populates="genre")


# ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──

class Film(Base):
    """
    Core film catalogue.
    `runtime_minutes` drives per-second analytics; `tmdb_id` links to external enrichment.
    """
    __tablename__ = "films"

    id: Mapped[int]                   = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    title: Mapped[str]                = mapped_column(String(255), nullable=False)
    original_title: Mapped[Optional[str]] = mapped_column(String(255))
    release_year: Mapped[Optional[int]]   = mapped_column(SmallInteger)
    runtime_minutes: Mapped[Optional[int]] = mapped_column(SmallInteger)
    language_code: Mapped[Optional[str]]   = mapped_column(String(10))     # BCP-47
    tmdb_id: Mapped[Optional[int]]         = mapped_column(Integer, unique=True)
    imdb_id: Mapped[Optional[str]]         = mapped_column(String(20),  unique=True)
    average_rating: Mapped[Optional[float]] = mapped_column(Float)
    synopsis: Mapped[Optional[str]]        = mapped_column(Text)
    poster_url: Mapped[Optional[str]]      = mapped_column(String(512))
    created_at: Mapped[datetime]           = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    session_films: Mapped[List["SessionFilm"]]  = relationship(back_populates="film")
    film_cast:     Mapped[List["FilmCast"]]     = relationship(back_populates="film")
    film_crew:     Mapped[List["FilmCrew"]]     = relationship(back_populates="film")
    film_genres:   Mapped[List["FilmGenre"]]    = relationship(back_populates="film")

    __table_args__ = (
        CheckConstraint("release_year BETWEEN 1888 AND 2100", name="ck_films_release_year"),
        CheckConstraint("runtime_minutes > 0",                name="ck_films_runtime_positive"),
        CheckConstraint("average_rating BETWEEN 0 AND 10",   name="ck_films_rating_range"),
        Index("ix_films_release_year",    "release_year"),
        Index("ix_films_language",        "language_code"),
        Index("ix_films_average_rating",  "average_rating"),
    )


# ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──

class Person(Base):
    """
    Unified cast + crew entity.
    A single row can appear in both FilmCast and FilmCrew,
    avoiding the classic actor/director duplication problem.
    """
    __tablename__ = "persons"

    id: Mapped[int]                   = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    full_name: Mapped[str]            = mapped_column(String(200), nullable=False)
    birth_year: Mapped[Optional[int]] = mapped_column(SmallInteger)
    nationality: Mapped[Optional[str]] = mapped_column(String(3))           # ISO-3166-1 alpha-3
    gender: Mapped[Optional[GenderEnum]] = mapped_column(
        Enum(GenderEnum, name="gender_enum"), nullable=True
    )
    tmdb_person_id: Mapped[Optional[int]] = mapped_column(Integer, unique=True)
    biography: Mapped[Optional[str]]      = mapped_column(Text)
    profile_image_url: Mapped[Optional[str]] = mapped_column(String(512))

    # Relationships
    film_cast: Mapped[List["FilmCast"]] = relationship(back_populates="person")
    film_crew: Mapped[List["FilmCrew"]] = relationship(back_populates="person")

    __table_args__ = (
        Index("ix_persons_full_name", "full_name"),       # trgm index added via migration
        Index("ix_persons_nationality", "nationality"),
    )


# ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──

class WatchSession(Base):
    """
    A single continuous viewing session for one user.
    Contains device metadata and timing; individual films
    are joined via SessionFilms with their view_order.
    """
    __tablename__ = "watch_sessions"

    id: Mapped[int]               = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int]          = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    started_at: Mapped[datetime]  = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    device_type: Mapped[Optional[WatchDeviceEnum]] = mapped_column(
        Enum(WatchDeviceEnum, name="watch_device_enum"), nullable=True
    )
    ip_country_code: Mapped[Optional[str]] = mapped_column(String(3))
    is_completed: Mapped[bool]    = mapped_column(Boolean, default=False, nullable=False)
    total_watch_seconds: Mapped[Optional[int]] = mapped_column(Integer)

    # Relationships
    user:          Mapped["User"]             = relationship(back_populates="sessions")
    session_films: Mapped[List["SessionFilm"]] = relationship(
        back_populates="session", order_by="SessionFilm.view_order"
    )

    __table_args__ = (
        CheckConstraint("ended_at IS NULL OR ended_at >= started_at", name="ck_session_time_order"),
        Index("ix_watch_sessions_user_id",    "user_id"),
        Index("ix_watch_sessions_started_at", "started_at"),
        # Composite: supports 'sessions per user in time range' queries
        Index("ix_watch_sessions_user_started", "user_id", "started_at"),
    )


# ─────────────────────────────────────────────
# 2.  JUNCTION TABLES
# ─────────────────────────────────────────────

class SessionFilm(Base):
    """
    WatchSession ↔ Film  (many-to-many)
    ──────────────────────────────────
    `view_order`     : 1-based position of the film inside the session.
    `watch_percent`  : 0–100, how far the user got through this film.
    `paused_count`   : pause events — a proxy for engagement quality.
    """
    __tablename__ = "session_films"

    session_id: Mapped[int]   = mapped_column(
        BigInteger, ForeignKey("watch_sessions.id", ondelete="CASCADE"), primary_key=True
    )
    film_id: Mapped[int]      = mapped_column(
        BigInteger, ForeignKey("films.id", ondelete="RESTRICT"), primary_key=True
    )
    view_order: Mapped[int]   = mapped_column(Integer, nullable=False)     # REQUIRED per spec
    watch_percent: Mapped[Optional[float]] = mapped_column(Float)
    paused_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Relationships
    session: Mapped["WatchSession"] = relationship(back_populates="session_films")
    film:    Mapped["Film"]         = relationship(back_populates="session_films")

    __table_args__ = (
        CheckConstraint("view_order >= 1",                      name="ck_sf_view_order_positive"),
        CheckConstraint("watch_percent BETWEEN 0 AND 100",      name="ck_sf_watch_percent_range"),
        CheckConstraint("paused_count >= 0",                    name="ck_sf_paused_count_non_neg"),

        # ── Indexing rationale ──────────────────────────────────────────────────
        # ix_sf_film_id  : drives self-join overlap queries
        #   "Find all users who watched film X AND film Y in the same session"
        #   → JOIN session_films sf1 ON sf1.film_id = X
        #     JOIN session_films sf2 ON sf2.session_id = sf1.session_id AND sf2.film_id = Y
        #   Both sides of the self-join hit this index.
        Index("ix_sf_film_id",              "film_id"),

        # Composite (film_id, session_id): covers the full self-join probe without
        # a heap fetch when only those two columns are needed.
        Index("ix_sf_film_session",         "film_id", "session_id"),

        # Composite (session_id, view_order): accelerates ordered scans within a session.
        Index("ix_sf_session_view_order",   "session_id", "view_order"),
    )


# ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──

class FilmCast(Base):
    """
    Film ↔ Person  (cast — many-to-many)
    `character_name` and `billing_order` allow top-N cast queries.
    """
    __tablename__ = "film_cast"

    film_id: Mapped[int]   = mapped_column(
        BigInteger, ForeignKey("films.id", ondelete="CASCADE"), primary_key=True
    )
    person_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("persons.id", ondelete="RESTRICT"), primary_key=True
    )
    character_name: Mapped[Optional[str]] = mapped_column(String(200))
    billing_order: Mapped[Optional[int]]  = mapped_column(SmallInteger)    # 1 = top-billed

    # Relationships
    film:   Mapped["Film"]   = relationship(back_populates="film_cast")
    person: Mapped["Person"] = relationship(back_populates="film_cast")

    __table_args__ = (
        CheckConstraint("billing_order >= 1", name="ck_fc_billing_order_positive"),

        # ix_fc_person_id: "All films starring person X" — core analytics query.
        Index("ix_fc_person_id",          "person_id"),

        # Composite covering index for self-join co-star overlap queries:
        #   "Find all actor pairs who appeared together in >= N films"
        #   → JOIN film_cast fc1 ON fc1.film_id = fc2.film_id
        #     GROUP BY fc1.person_id, fc2.person_id
        Index("ix_fc_person_film",        "person_id", "film_id"),

        # Supports "top-billed cast for film" without sorting heap pages
        Index("ix_fc_film_billing",       "film_id", "billing_order"),
    )


# ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──

class FilmCrew(Base):
    """
    Film ↔ Person  (crew — many-to-many)
    A person can hold multiple roles on the same film
    (e.g. writer + director), so the PK is (film_id, person_id, role).
    """
    __tablename__ = "film_crew"

    film_id: Mapped[int]   = mapped_column(
        BigInteger, ForeignKey("films.id", ondelete="CASCADE"), primary_key=True
    )
    person_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("persons.id", ondelete="RESTRICT"), primary_key=True
    )
    role: Mapped[CrewRoleEnum] = mapped_column(
        Enum(CrewRoleEnum, name="crew_role_enum"), primary_key=True, nullable=False
    )
    department: Mapped[Optional[str]] = mapped_column(String(80))

    # Relationships
    film:   Mapped["Film"]   = relationship(back_populates="film_crew")
    person: Mapped["Person"] = relationship(back_populates="film_crew")

    __table_args__ = (
        # "All films directed by person X"
        Index("ix_fcrew_person_id",        "person_id"),

        # Composite: role filter without film-heap fetch
        # e.g. "All directors across the catalogue"
        Index("ix_fcrew_role_person",      "role", "person_id"),

        # Self-join: "Directors who share >= N films as collaborators"
        Index("ix_fcrew_person_film_role", "person_id", "film_id", "role"),
    )


# ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──

class FilmGenre(Base):
    """
    Film ↔ Genre  (many-to-many)
    Intentionally lean — genre data changes rarely,
    so indexes are kept minimal but precisely targeted.
    """
    __tablename__ = "film_genres"

    film_id: Mapped[int]  = mapped_column(
        BigInteger,  ForeignKey("films.id",   ondelete="CASCADE"),  primary_key=True
    )
    genre_id: Mapped[int] = mapped_column(
        SmallInteger, ForeignKey("genres.id", ondelete="RESTRICT"), primary_key=True
    )
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Relationships
    film:  Mapped["Film"]  = relationship(back_populates="film_genres")
    genre: Mapped["Genre"] = relationship(back_populates="film_genres")

    __table_args__ = (
        # "All films in genre X" — the dominant lookup direction
        Index("ix_fg_genre_id",          "genre_id"),

        # Composite: genre + film for self-join genre-overlap queries
        #   "Find users whose sessions overlap >= 2 genres"
        Index("ix_fg_genre_film",        "genre_id", "film_id"),

        # Partial index: quickly identify the primary genre per film
        Index("ix_fg_film_primary",      "film_id",
              postgresql_where="is_primary = TRUE"),
    )


# ─────────────────────────────────────────────
# 3.  DDL Helper  (optional — for quick setup)
# ─────────────────────────────────────────────

if __name__ == "__main__":
    from sqlalchemy import create_engine

    # Replace with your actual DSN; echo=True prints generated SQL
    DATABASE_URL = "postgresql+psycopg2://postgres:postgres123@localhost:5433/media_analytics"
    engine = create_engine(DATABASE_URL, echo=True)
    Base.metadata.create_all(engine)
    print("✅  Schema created successfully.")