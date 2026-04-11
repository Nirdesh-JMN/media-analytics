CREATE TABLE IF NOT EXISTS films (
    id SERIAL PRIMARY KEY,
    tmdb_id INT UNIQUE,
    imdb_id TEXT,
    title TEXT,
    original_title TEXT,
    release_year INT,
    runtime_minutes INT,
    language_code TEXT,
    average_rating FLOAT,
    synopsis TEXT,
    poster_url TEXT
);

-- GENRES
CREATE TABLE IF NOT EXISTS genres (
    id SERIAL PRIMARY KEY,
    name TEXT,
    slug TEXT UNIQUE
);

-- FILM GENRES
CREATE TABLE IF NOT EXISTS film_genres (
    film_id INT REFERENCES films(id) ON DELETE CASCADE,
    genre_id INT REFERENCES genres(id) ON DELETE CASCADE,
    is_primary BOOLEAN,
    PRIMARY KEY (film_id, genre_id)
);

-- PERSONS
CREATE TABLE IF NOT EXISTS persons (
    id SERIAL PRIMARY KEY,
    tmdb_person_id INT UNIQUE,
    full_name TEXT,
    profile_image_url TEXT
);

-- FILM CAST
CREATE TABLE IF NOT EXISTS film_cast (
    film_id INT REFERENCES films(id) ON DELETE CASCADE,
    person_id INT REFERENCES persons(id) ON DELETE CASCADE,
    character_name TEXT,
    billing_order INT,
    PRIMARY KEY (film_id, person_id)
);

-- CREW ENUM
DO $$ BEGIN
    CREATE TYPE crew_role_enum AS ENUM (
        'director','producer','writer','cinematographer',
        'editor','composer','production_design',
        'vfx_supervisor','other'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- FILM CREW
CREATE TABLE IF NOT EXISTS film_crew (
    film_id INT REFERENCES films(id) ON DELETE CASCADE,
    person_id INT REFERENCES persons(id) ON DELETE CASCADE,
    role crew_role_enum,
    department TEXT,
    PRIMARY KEY (film_id, person_id, role)
);

-- USERS
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT,
    email TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- WATCH SESSIONS
CREATE TABLE IF NOT EXISTS watch_sessions (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    is_completed BOOLEAN DEFAULT FALSE
);

-- SESSION FILMS
CREATE TABLE IF NOT EXISTS session_films (
    session_id INT REFERENCES watch_sessions(id) ON DELETE CASCADE,
    film_id INT REFERENCES films(id) ON DELETE CASCADE,
    view_order INT,
    paused_count INT DEFAULT 0,
    PRIMARY KEY (session_id, film_id)
);