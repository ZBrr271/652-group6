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
    mbid VARCHAR(36) PRIMARY KEY,       
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
    artist VARCHAR(255),
    song_name VARCHAR(255),
    duration INT,
    listeners INT,
    playcount INT,
    mbid VARCHAR(255) NOT NULL,
    album_name VARCHAR(255),
    url VARCHAR(255),
    tag_ranks JSONB,
    toptags TEXT[],
    wiki_summary TEXT
);

