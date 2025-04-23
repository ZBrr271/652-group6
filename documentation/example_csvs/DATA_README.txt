Description of data files:

"hot-100-current.csv" - This is generated from the API call to Kaggle - from "kaggle_dag"

"unique_hot100_XX.csv" - The transformed Hot 100 data - from "kaggle_dag"

"spotify_tracks_XX.csv" - Raw Spotify response data - from "spotify_dag"

"spotify_tracks_cleaned_XX.csv" - The transformed Spotify track data - from "spotify_dag"

"lastfm_clean_XX.csv" - The transformed Last.fm track data - from "lastfm_brainz_dag"

"acousticbrainz_features_XX.csv" - The acousticbrainz track data - from "lastfm_brainz_dag"

"spottokaggle_match_results_XX.csv" - Looks for matches from spot tracks to billboard data by top artist and song name beyond a perfect match - from "matching_dag"
Some matches may be missed but thresholds are selected to make sure not to pick up any non-matches. Match percentages below a certain threshold are not even shown

"spottolastfm_match_results_XX.csv" - Looks for matches from spot tracks to last.fm tracks by top artist and song name beyond a perfect match - from "matching_dag"
Some matches may be missed but thresholds are selected to make sure not to pick up any non-matches. Match percentages below a certain threshold are not even shown




Example pgAdmin query to see combined data from tables

SELECT
    s.*,
    b.*,
    l.*,
    a.*
FROM
    spotify_tracks s
JOIN
    billboard_chart_data b ON s.group6_id = b.group6_id
JOIN
    lastfm_tracks l ON s.group6_id = l.group6_id
JOIN
    acousticbrainz_features a ON s.group6_id = a.group6_id;



Or view any of the tables individually:

SELECT * FROM billboard_chart_data;

SELECT * FROM spotify_tracks;

SELECT * from lastfm_tracks;

SELECT * FROM acousticbrainz_features;