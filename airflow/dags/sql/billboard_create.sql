-- create table for billboard chart data
-- sourced from kaggle
DROP TABLE IF EXISTS billboard_chart_data;
CREATE TABLE billboard_chart_data (
    artists VARCHAR(255),
    song_name VARCHAR(255),
    peak_chart_pos INT,
    last_chart_pos INT,
    total_wks_on_chart INT,
    wk_last_charted DATE
);
