-- drop all tables if they exist
DROP TABLE IF EXISTS lastfm_top_tags_z;
DROP TABLE IF EXISTS lastfm_tracks_z;

-- create table for lastfm tags
CREATE TABLE lastfm_top_tags_z (
    tag_name VARCHAR(255) PRIMARY KEY,
    tag_count INT,
    tag_reach INT
);

-- create table for lastfm tag tracks
-- contains nulls for some track_mbid, so we can't use it as a primary key
CREATE TABLE lastfm_tracks_z (
    artist VARCHAR(255),
    song_name VARCHAR(255),
    duration INT,
    listeners INT,
    playcount INT,
    mbid VARCHAR(255) NOT NULL,
    album_name VARCHAR(255),
    url VARCHAR(255),
    tag_ranks JSONB,
    toptags TEXT,
    wiki_summary TEXT
);