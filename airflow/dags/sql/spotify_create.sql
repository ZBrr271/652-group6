-- create table for spotify tracks
-- pulled from playlist data
DROP TABLE IF EXISTS spotify_tracks;
CREATE TABLE spotify_tracks (
    top_artist VARCHAR(255),
    artists VARCHAR(255),
    song_name VARCHAR(255),
    duration INT,
    popularity INT,
    spotify_id VARCHAR(100),
    album_name VARCHAR(255),
    album_id VARCHAR(100),
    album_release_date DATE,
    album_release_date_precision VARCHAR(100),
    album_image VARCHAR(100),
    explicit_lyrics BOOLEAN,
    isrc VARCHAR(100),
    spotify_url VARCHAR(100),
    available_markets TEXT

);
