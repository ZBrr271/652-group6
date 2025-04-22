from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from rapidfuzz import process, fuzz
import numpy as np
import time
from datetime import datetime
from math import sqrt
import os


def load_all_from_db():

    pg_hook = PostgresHook(postgres_conn_id='pg_group6')

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM spotify_tracks")
            df_spot = cur.fetch_all()

            cur.execute("SELECT * FROM billboard_chart_data")
            df_kag = cur.fetch_all()

            cur.execute("SELECT * FROM lastfm_tracks")
            df_lastfm = cur.fetch_all()

            cur.execute("SELECT * FROM acousticbrainz_features")
            df_ab = cur.fetch_all()

    df_spot = pd.DataFrame(df_spot, columns=df_spot[0].keys())
    df_kag = pd.DataFrame(df_kag, columns=df_kag[0].keys())
    df_lastfm = pd.DataFrame(df_lastfm, columns=df_lastfm[0].keys())

    return df_spot, df_kag, df_lastfm, df_ab


def append_match_result(results, spot_artists, spot_song_name, other_artists, other_song_name, 
                       match_found, other_index, spot_index, match_score, 
                       artist_match_pct, name_match_pct):
    results.append({
        'spot_artists': spot_artists,
        'spot_song_name': spot_song_name,
        'other_artists': other_artists,
        'other_song_name': other_song_name,
        'match_found': match_found,
        'other_index': other_index,
        'spot_index': spot_index,
        'best_match_score': match_score,
        'best_match_artist_match_pct': artist_match_pct,
        'best_match_name_match_pct': name_match_pct
    })
    

def match_spot_to_kag(df_spot, df_kag):

    df_kag = df_kag[df_kag['peak_chart_pos'] <= 20]
    df_kag = df_kag.reset_index(drop=True)

    print(f"Filtered Kaggle dataset to {len(df_kag)} records with peak chart position <= 20")

    df_kag['has_spot_match'] = False
    df_kag['spot_match_index'] = None
    df_kag['exact_match'] = False
    df_spot['has_kag_match'] = False
    df_spot['kag_match_index'] = None

    results = []

    start_time = time.time()

    kag_performers = set(df_kag['top_artist'].unique())
    performer_indices = {performer: df_kag[df_kag['top_artist'] == performer].index[0] 
                        for performer in kag_performers}

    performer_to_titles = {}
    for idx, row in df_kag.iterrows():
        performer = row['top_artist']
        title = row['song_name']
        if performer not in performer_to_titles:
            performer_to_titles[performer] = set()
        performer_to_titles[performer].add(title)

    kag_performer_title_to_index = {}
    for idx, row in df_kag.iterrows():
        performer = row['top_artist']
        title = row['song_name']
        kag_performer_title_to_index[(performer, title)] = idx

    # Pre-compute word sets
    kag_artist_words_dict = {idx: set(row['top_artist'].split()) for idx, row in df_kag.iterrows()}
    kag_title_words_dict = {idx: set(row['song_name'].split()) for idx, row in df_kag.iterrows()}

    # Create artist+first letter of title index
    artist_title_first = {}
    for idx, row in df_kag.iterrows():
        if row['song_name']:
            key = (row['top_artist'], row['song_name'][0])
            if key not in artist_title_first:
                artist_title_first[key] = []
            artist_title_first[key].append(idx)

    for index, row in df_spot.iterrows():
        if df_spot.at[index, 'has_kag_match']:
            continue

        artist = row['top_artist']
        song_name = row['song_name']
        match_found = False

        best_match_performer = None
        best_match_title = None
        best_match_score = 0
        best_match_artist_match_pct = 0
        best_match_name_match_pct = 0
        best_match_kag_index = None

        if index % 10 == 0:
            print(f"Processing record {index + 1} of {len(df_spot)}...")
        
        if artist in kag_performers:
            curr_performer = artist
            
            if song_name in performer_to_titles.get(curr_performer, set()):
                kag_index = kag_performer_title_to_index.get((curr_performer, song_name))
                best_match_kag_index = kag_index

                # Update both dataframes
                df_spot.at[index, 'has_kag_match'] = True
                df_spot.at[index, 'kag_match_index'] = kag_index
                df_kag.at[kag_index, 'has_spot_match'] = True
                df_kag.at[kag_index, 'spot_match_index'] = index
                df_kag.at[kag_index, 'exact_match'] = True

                match_found = True
                append_match_result(results, artist, song_name, curr_performer, song_name, 
                                    match_found, best_match_kag_index, index, 100, 100, 100)
                continue

            start_index = performer_indices[curr_performer]

            for i in range(start_index, len(df_kag)):

                if df_kag.at[i, 'has_spot_match']:
                    continue

                curr_performer = df_kag.iloc[i]['top_artist']
                title = df_kag.iloc[i]['song_name']

                if curr_performer[0] != artist[0]:
                    continue
                if title[0] != song_name[0]:
                    continue

                spot_artist_words = set(artist.split())
                curr_artist_words = kag_artist_words_dict[i]
                if not spot_artist_words.intersection(curr_artist_words):
                    continue 

                spot_title_words = set(song_name.split())
                curr_title_words = kag_title_words_dict[i]
                if not spot_title_words.intersection(curr_title_words):
                    continue 


                artist_score = fuzz.QRatio(artist, curr_performer)
                if artist_score < 60:
                    continue
                title_score = fuzz.QRatio(song_name, title)
                if title_score < 60:
                    continue
                geo_mean_score = sqrt(artist_score * title_score)

                if geo_mean_score > best_match_score:
                    best_match_performer = curr_performer
                    best_match_title = title
                    best_match_score = geo_mean_score
                    best_match_artist_match_pct = artist_score
                    best_match_name_match_pct = title_score
                    best_match_kag_index = i

                if geo_mean_score >= 70:
                    match_found = True

                    kag_index = i
                    df_spot.at[index, 'has_kag_match'] = True
                    df_spot.at[index, 'kag_match_index'] = kag_index
                    df_kag.at[kag_index, 'has_spot_match'] = True
                    df_kag.at[kag_index, 'spot_match_index'] = index

                    append_match_result(results, artist, song_name, curr_performer, title, 
                                        match_found, best_match_kag_index, index, best_match_score,
                                        best_match_artist_match_pct, best_match_name_match_pct)
                    break

            if not match_found:
                for i in range(0, start_index):
                    if df_kag.at[i, 'has_spot_match']:
                        continue

                    curr_performer = df_kag.iloc[i]['top_artist']
                    title = df_kag.iloc[i]['song_name']

                    if curr_performer[0] != artist[0]:
                        continue
                    if title[0] != song_name[0]:
                        continue

                    spot_artist_words = set(artist.split())
                    curr_artist_words = kag_artist_words_dict[i]
                    if not spot_artist_words.intersection(curr_artist_words):
                        continue 

                    spot_title_words = set(song_name.split())
                    curr_title_words = kag_title_words_dict[i]
                    if not spot_title_words.intersection(curr_title_words):
                        continue 

                    artist_score = fuzz.QRatio(artist, curr_performer)
                    if artist_score < 60:
                        continue  # Skip early
                    
                    # Only calculate title score if artist score is promising
                    title_score = fuzz.QRatio(song_name, title)
                    if title_score < 60:
                        continue  # Skip early

                    geo_mean_score = sqrt(artist_score * title_score)

                    if geo_mean_score > best_match_score:
                        best_match_performer = curr_performer
                        best_match_title = title
                        best_match_score = geo_mean_score
                        best_match_artist_match_pct = artist_score
                        best_match_name_match_pct = title_score
                        best_match_kag_index = i

                    if geo_mean_score >= 70:
                        match_found = True
                        kag_index = i
                        df_spot.at[index, 'has_kag_match'] = True
                        df_spot.at[index, 'kag_match_index'] = kag_index
                        df_kag.at[kag_index, 'has_spot_match'] = True
                        df_kag.at[kag_index, 'spot_match_index'] = index


                        append_match_result(results, artist, song_name, curr_performer, title, 
                                            match_found, best_match_kag_index, index, best_match_score,
                                            best_match_artist_match_pct, best_match_name_match_pct)
                        break

        else:
            for i in range(0, len(df_kag)):
                if df_kag.at[i, 'has_spot_match']:
                    continue

                curr_performer = df_kag.iloc[i]['top_artist']
                title = df_kag.iloc[i]['song_name']

                if curr_performer[0] != artist[0]:
                    continue
                if title[0] != song_name[0]:
                    continue

                spot_artist_words = set(artist.split())
                curr_artist_words = kag_artist_words_dict[i]
                if not spot_artist_words.intersection(curr_artist_words):
                    continue 

                spot_title_words = set(song_name.split())
                curr_title_words = kag_title_words_dict[i]
                if not spot_title_words.intersection(curr_title_words):
                    continue 

                artist_score = fuzz.QRatio(artist, curr_performer)
                if artist_score < 60:
                    continue
                title_score = fuzz.QRatio(song_name, title)
                if title_score < 60:
                    continue
                geo_mean_score = sqrt(artist_score * title_score)

                if geo_mean_score > best_match_score:
                    best_match_performer = curr_performer
                    best_match_title = title
                    best_match_score = geo_mean_score
                    best_match_artist_match_pct = artist_score
                    best_match_name_match_pct = title_score
                    best_match_kag_index = i

                if geo_mean_score >= 70:
                    match_found = True
                    kag_index = i
                    df_spot.at[index, 'has_kag_match'] = True
                    df_spot.at[index, 'kag_match_index'] = kag_index
                    df_kag.at[kag_index, 'has_spot_match'] = True
                    df_kag.at[kag_index, 'spot_match_index'] = index


                    append_match_result(results, artist, song_name, curr_performer, title, 
                                        match_found, best_match_kag_index, index, best_match_score,
                                        best_match_artist_match_pct, best_match_name_match_pct)
                    break

        if not match_found:
            append_match_result(results, artist, song_name, best_match_performer, best_match_title, 
                                match_found, best_match_kag_index, index, best_match_score,
                                best_match_artist_match_pct, best_match_name_match_pct)

    
    for index, row in df_spot.iterrows():
        if df_spot.at[index, 'has_kag_match']:
            if df_spot.at[index, 'exact_match']:
                continue
            else:
                df_kag.at[df_spot.at[index, 'kag_match_index'], 'group6_id'] = df_spot.at[index, 'group6_id']

    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\nTotal time taken for Spotify and Kaggle matching: {elapsed_time:.2f} seconds")
    
    results_df = pd.DataFrame(results)
    results_df.sort_values(by=['best_match_score'], inplace=True)
    results_df.reset_index(drop=True, inplace=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    match_results_file_name = f"spottokaggle_match_results_{timestamp}.csv"
    data_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(data_dir, exist_ok=True)
    match_results_file_path = os.path.join(data_dir, match_results_file_name)
    results_df.to_csv(match_results_file_path, index=False)

    print(f"Successfully wrote to {match_results_file_name}\n")


    return df_spot, df_kag      
                

def match_spot_to_lastfm(df_spot, df_lastfm):

    start_time = time.time()

    df_lastfm['has_spot_match'] = False
    df_lastfm['spot_match_index'] = None
    df_lastfm['exact_match'] = False
    df_spot['has_lastfm_match'] = False
    df_spot['lastfm_match_index'] = None

    results = []

    start_time = time.time()

    lastfm_artists = set(df_lastfm['artist'].unique())
    artist_indices = {artist: df_lastfm[df_lastfm['artist'] == artist].index[0] 
                        for artist in lastfm_artists}

    artist_to_titles = {}
    for idx, row in df_lastfm.iterrows():
        artist = row['artist']
        title = row['song_name']
        if artist not in artist_to_titles:
            artist_to_titles[artist] = set()
        artist_to_titles[artist].add(title)

    lastfm_artist_title_to_index = {}
    for idx, row in df_lastfm.iterrows():
        artist = row['artist']
        title = row['song_name']
        lastfm_artist_title_to_index[(artist, title)] = idx

    # Pre-compute word sets
    lastfm_artist_words_dict = {idx: set(row['artist'].split()) for idx, row in df_lastfm.iterrows()}
    lastfm_title_words_dict = {idx: set(row['song_name'].split()) for idx, row in df_lastfm.iterrows()}

    # Create artist+first letter of title index
    artist_title_first = {}
    for idx, row in df_lastfm.iterrows():
        if row['song_name']:
            key = (row['artist'], row['song_name'][0])
            if key not in artist_title_first:
                artist_title_first[key] = []
            artist_title_first[key].append(idx)

    for index, row in df_spot.iterrows():
        if df_spot.at[index, 'has_lastfm_match']:
            continue

        artist = row['top_artist']
        song_name = row['song_name']
        match_found = False

        best_match_performer = None
        best_match_title = None
        best_match_score = 0
        best_match_artist_match_pct = 0
        best_match_name_match_pct = 0
        best_match_kag_index = None

        if index % 10 == 0:
            print(f"Processing record {index + 1} of {len(df_spot)}...")
        
        if artist in lastfm_artists:
            curr_artist = artist
            
            if song_name in artist_to_titles.get(curr_artist, set()):
                lastfm_index = lastfm_artist_title_to_index.get((curr_artist, song_name))
                best_match_lastfm_index = lastfm_index

                # Update both dataframes
                df_spot.at[index, 'has_lastfm_match'] = True
                df_spot.at[index, 'lastfm_match_index'] = lastfm_index
                df_lastfm.at[lastfm_index, 'has_spot_match'] = True
                df_lastfm.at[lastfm_index, 'spot_match_index'] = index
                df_lastfm.at[lastfm_index, 'exact_match'] = True

                match_found = True
                append_match_result(results, artist, song_name, curr_artist, song_name, 
                                    match_found, best_match_lastfm_index, index, 100, 100, 100)
                continue

            start_index = artist_indices[curr_artist]

            for i in range(start_index, len(df_lastfm)):

                if df_lastfm.at[i, 'has_spot_match']:
                    continue

                curr_artist = df_lastfm.iloc[i]['artist']
                title = df_lastfm.iloc[i]['song_name']

                if curr_artist[0] != artist[0]:
                    continue
                if title[0] != song_name[0]:
                    continue

                spot_artist_words = set(artist.split())
                curr_artist_words = lastfm_artist_words_dict[i]
                if not spot_artist_words.intersection(curr_artist_words):
                    continue 

                spot_title_words = set(song_name.split())
                curr_title_words = lastfm_title_words_dict[i]
                if not spot_title_words.intersection(curr_title_words):
                    continue 


                artist_score = fuzz.QRatio(artist, curr_artist)
                if artist_score < 60:
                    continue
                title_score = fuzz.QRatio(song_name, title)
                if title_score < 60:
                    continue
                geo_mean_score = sqrt(artist_score * title_score)

                if geo_mean_score > best_match_score:
                    best_match_performer = curr_artist
                    best_match_title = title
                    best_match_score = geo_mean_score
                    best_match_artist_match_pct = artist_score
                    best_match_name_match_pct = title_score
                    best_match_lastfm_index = i

                if geo_mean_score >= 70:
                    match_found = True

                    lastfm_index = i
                    df_spot.at[index, 'has_lastfm_match'] = True
                    df_spot.at[index, 'lastfm_match_index'] = lastfm_index
                    df_lastfm.at[lastfm_index, 'has_spot_match'] = True
                    df_lastfm.at[lastfm_index, 'spot_match_index'] = index

                    append_match_result(results, artist, song_name, curr_artist, title, 
                                        match_found, best_match_lastfm_index, index, best_match_score,
                                        best_match_artist_match_pct, best_match_name_match_pct)
                    break

            if not match_found:
                for i in range(0, start_index):
                    if df_lastfm.at[i, 'has_spot_match']:
                        continue

                    curr_artist = df_lastfm.iloc[i]['artist']
                    title = df_lastfm.iloc[i]['song_name']

                    if curr_artist[0] != artist[0]:
                        continue
                    if title[0] != song_name[0]:
                        continue

                    spot_artist_words = set(artist.split())
                    curr_artist_words = lastfm_artist_words_dict[i]
                    if not spot_artist_words.intersection(curr_artist_words):
                        continue 

                    spot_title_words = set(song_name.split())
                    curr_title_words = lastfm_title_words_dict[i]
                    if not spot_title_words.intersection(curr_title_words):
                        continue 

                    artist_score = fuzz.QRatio(artist, curr_artist)
                    if artist_score < 60:
                        continue  # Skip early
                    
                    # Only calculate title score if artist score is promising
                    title_score = fuzz.QRatio(song_name, title)
                    if title_score < 60:
                        continue  # Skip early

                    geo_mean_score = sqrt(artist_score * title_score)

                    if geo_mean_score > best_match_score:
                        best_match_performer = curr_artist
                        best_match_title = title
                        best_match_score = geo_mean_score
                        best_match_artist_match_pct = artist_score
                        best_match_name_match_pct = title_score
                        best_match_lastfm_index = i

                    if geo_mean_score >= 70:
                        match_found = True
                        lastfm_index = i
                        df_spot.at[index, 'has_lastfm_match'] = True
                        df_spot.at[index, 'lastfm_match_index'] = lastfm_index
                        df_lastfm.at[lastfm_index, 'has_spot_match'] = True
                        df_lastfm.at[lastfm_index, 'spot_match_index'] = index


                        append_match_result(results, artist, song_name, curr_artist, title, 
                                            match_found, best_match_lastfm_index, index, best_match_score,
                                            best_match_artist_match_pct, best_match_name_match_pct)
                        break

        else:
            for i in range(0, len(df_lastfm)):
                if df_lastfm.at[i, 'has_spot_match']:
                    continue

                curr_artist = df_lastfm.iloc[i]['artist']
                title = df_lastfm.iloc[i]['song_name']

                if curr_artist[0] != artist[0]:
                    continue
                if title[0] != song_name[0]:
                    continue

                spot_artist_words = set(artist.split())
                curr_artist_words = lastfm_artist_words_dict[i]
                if not spot_artist_words.intersection(curr_artist_words):
                    continue 

                spot_title_words = set(song_name.split())
                curr_title_words = lastfm_title_words_dict[i]
                if not spot_title_words.intersection(curr_title_words):
                    continue 

                artist_score = fuzz.QRatio(artist, curr_artist)
                if artist_score < 60:
                    continue
                title_score = fuzz.QRatio(song_name, title)
                if title_score < 60:
                    continue
                geo_mean_score = sqrt(artist_score * title_score)

                if geo_mean_score > best_match_score:
                    best_match_performer = curr_artist
                    best_match_title = title
                    best_match_score = geo_mean_score
                    best_match_artist_match_pct = artist_score
                    best_match_name_match_pct = title_score
                    best_match_lastfm_index = i

                if geo_mean_score >= 70:
                    match_found = True
                    lastfm_index = i
                    df_spot.at[index, 'has_lastfm_match'] = True
                    df_spot.at[index, 'lastfm_match_index'] = lastfm_index
                    df_lastfm.at[lastfm_index, 'has_spot_match'] = True
                    df_lastfm.at[lastfm_index, 'spot_match_index'] = index


                    append_match_result(results, artist, song_name, curr_artist, title, 
                                        match_found, best_match_lastfm_index, index, best_match_score,
                                        best_match_artist_match_pct, best_match_name_match_pct)
                    break

        if not match_found:
            append_match_result(results, artist, song_name, best_match_performer, best_match_title, 
                                match_found, best_match_lastfm_index, index, best_match_score,
                                best_match_artist_match_pct, best_match_name_match_pct)

    
    for index, row in df_spot.iterrows():
        if df_spot.at[index, 'has_lastfm_match']:
            if df_spot.at[index, 'exact_match']:
                continue
            else:
                df_lastfm.at[df_spot.at[index, 'lastfm_match_index'], 'group6_id'] = df_spot.at[index, 'group6_id']


    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\nTotal time taken for Spotify and LastFM matching: {elapsed_time:.2f} seconds")

    results_df = pd.DataFrame(results)
    results_df.sort_values(by=['best_match_score'], inplace=True)
    results_df.reset_index(drop=True, inplace=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    match_results_file_name = f"spottolastfm_match_results_{timestamp}.csv"
    data_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(data_dir, exist_ok=True)
    match_results_file_path = os.path.join(data_dir, match_results_file_name)
    results_df.to_csv(match_results_file_path, index=False)

    print(f"Successfully wrote to {match_results_file_name}\n")



def match_all_tracks():
    df_spot, df_kag, df_lastfm, df_ab = load_all_from_db()

    df_spot, df_kag = match_spot_to_kag(df_spot, df_kag)

    df_spot, df_lastfm = match_spot_to_lastfm(df_spot, df_lastfm)

    
match_all_tracks()




