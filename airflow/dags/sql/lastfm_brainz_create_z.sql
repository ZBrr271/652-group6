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
    artist VARCHAR(255),
    song_name VARCHAR(255),
    album VARCHAR(255),
    date VARCHAR(255),
    isrcs JSONB,                         
    bpm NUMERIC,
    initialkey VARCHAR(255),
    musicbrainz_albumid VARCHAR(255),
    musicbrainz_artistid VARCHAR(255),
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



