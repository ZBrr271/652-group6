-- drop all tables if they exist
DROP TABLE IF EXISTS lastfm_top_tag_tracks;
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
CREATE TABLE lastfm_top_tag_tracks (
    track_name VARCHAR(255),
    track_mbid VARCHAR(255),
    artist_name VARCHAR(255),
    artist_mbid VARCHAR(255),
    tag_name VARCHAR(255),
    tag_rank INT,
    PRIMARY KEY (track_name, artist_name, tag_name),
    CONSTRAINT fk_tag_name FOREIGN KEY (tag_name) REFERENCES lastfm_top_tags(tag_name)
);

