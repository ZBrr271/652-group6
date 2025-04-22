-- 685.652, Spring 2025 - Group 6 Final Project
-- lastfm_brainz_create_z.sql

-- drop all tables if they exist
DROP TABLE IF EXISTS lastfm_tracks;
DROP TABLE IF EXISTS acousticbrainz_features;
DROP TABLE IF EXISTS lastfm_top_tags;

-- create table for lastfm tags
CREATE TABLE lastfm_top_tags (
    tag_name VARCHAR(255) PRIMARY KEY,
    tag_count INT,
    tag_reach INT
);


-- create table for acousticbrainz features
CREATE TABLE acousticbrainz_features (
    mbid TEXT PRIMARY KEY,
    group6_id TEXT,
    artist JSONB,
    song_name JSONB,
    album JSONB,
    date JSONB,
    isrcs JSONB,                         
    bpm JSONB,
    initialkey TEXT,
    musicbrainz_albumid TEXT,
    musicbrainz_artistid TEXT,
    mood JSONB,
    danceability_danceable FLOAT,
    danceability_not_danceable FLOAT,
    danceability_max_class VARCHAR(255),
    gender_female FLOAT,
    gender_male FLOAT,
    gender_max_class VARCHAR(255),
    genre_alternative FLOAT,
    genre_blues FLOAT,
    genre_electronic FLOAT,
    genre_folkcountry FLOAT,    
    genre_funksoulrnb FLOAT,
    genre_jazz FLOAT,
    genre_pop FLOAT,
    genre_raphiphop FLOAT,
    genre_rock FLOAT,
    genre_dortmund_maxclass VARCHAR(255),  
    voice_instrumental_instrumental FLOAT,
    voice_instrumental_voice FLOAT,
    voice_instrumental_max_class VARCHAR(255),
    genre TEXT                   
);


-- create table for lastfm tag tracks
CREATE TABLE lastfm_tracks (
    mbid TEXT PRIMARY KEY,
    group6_id TEXT,
    artist TEXT,
    song_name TEXT,
    duration INT,
    listeners INT,
    playcount INT,
    album_name TEXT,
    url TEXT,
    tag_ranks JSONB,
    toptags TEXT[],
    wiki_summary TEXT
);
