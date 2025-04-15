-- create table for lastfm tags
DROP TABLE IF EXISTS lastfm_top_tags;
CREATE TABLE lastfm_top_tags (
    tag_name VARCHAR(255) PRIMARY KEY,
    tag_count INT,
    tag_reach INT
);

-- create table for lastfm tag tracks
-- contains nulls for some track_mbid, so we can't use it as a primary key
DROP TABLE IF EXISTS lastfm_top_tag_tracks;
CREATE TABLE lastfm_top_tag_tracks (
    track_name VARCHAR(255),
    track_mbid VARCHAR(255),
    artist_name VARCHAR(255),
    artist_mbid VARCHAR(255),
    tag VARCHAR(255),
    rank INT,
    PRIMARY KEY (track_name, artist_name, tag)
);