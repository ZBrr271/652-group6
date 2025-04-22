-- 685.652, Spring 2025 - Group 6 Final Project
-- billboard_create.sql

-- create table for billboard chart data
-- dataset is sourced from kaggle
DROP TABLE IF EXISTS billboard_chart_data;
CREATE TABLE billboard_chart_data (
    group6_id TEXT PRIMARY KEY,
    top_artist TEXT,
    artists TEXT[],
    song_name TEXT,
    peak_chart_pos INT,
    last_chart_pos INT,
    total_wks_on_chart INT,
    wk_first_charted DATE,
    wk_last_charted DATE
);