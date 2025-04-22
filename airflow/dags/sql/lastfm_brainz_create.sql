-- drop all tables if they exist
DROP TABLE IF EXISTS lastfm_tracks;
DROP TABLE IF EXISTS acousticbrainz_features;
DROP TABLE IF EXISTS lastfm_top_tags;

-- create table for lastfm tags
CREATE TABLE lastfm_top_tags (
    tag_name TEXT PRIMARY KEY,
    tag_count INT,
    tag_reach INT
);

-- create table for acousticbrainz features
CREATE TABLE acousticbrainz_features (
    mbid TEXT PRIMARY KEY,
    group6_id TEXT,
    isrcs TEXT[],                         
    danceability NUMBER,
    genre_alternative NUMBER,
    genre_blues NUMBER,
    genre_electronic NUMBER,
    genre_folkcountry NUMBER,
    genre_funksoulrnb NUMBER,
    genre_jazz NUMBER,
    genre_pop NUMBER,
    genre_raphiphop NUMBER,
    genre_rock NUMBER,
    mood_happy NUMBER,
    mood_party NUMBER,
    mood_relaxed NUMBER,
    mood_sad NUMBER,
    metadata JSONB                       
);

-- create table for lastfm tag tracks
-- contains nulls for some track_mbid, so we can't use it as a primary key
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

