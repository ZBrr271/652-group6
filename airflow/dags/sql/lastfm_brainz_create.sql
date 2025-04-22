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
    danceability FLOAT,
    genre_alternative FLOAT,
    genre_blues FLOAT,
    genre_electronic FLOAT,
    genre_folkcountry FLOAT,
    genre_funksoulrnb FLOAT,
    genre_jazz FLOAT,
    genre_pop FLOAT,
    genre_raphiphop FLOAT,
    genre_rock FLOAT,
    mood_happy FLOAT,
    mood_party FLOAT,
    mood_relaxed FLOAT,
    mood_sad FLOAT,
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

