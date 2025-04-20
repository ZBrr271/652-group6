-- create table for spotify tracks
-- pulled from playlist data
DROP TABLE IF EXISTS spotify_tracks;
CREATE TABLE spotify_tracks (
    group6_id TEXT PRIMARY KEY,
    top_artist TEXT,
    artists TEXT[],
    song_name TEXT,
    duration INT,
    popularity INT,
    spotify_id TEXT,
    album_name TEXT,
    album_id TEXT,
    album_release_date DATE,
    album_release_date_precision TEXT,
    album_image TEXT,
    explicit_lyrics BOOLEAN,
    isrc TEXT,
    spotify_url TEXT,
    available_markets TEXT

);
