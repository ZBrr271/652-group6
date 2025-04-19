import pandas as pd
from rapidfuzz import process, fuzz
import numpy as np
import time
from datetime import datetime
from math import sqrt
import os


def append_match_result(results, spot_artists, spot_song_name, kag_artists, kag_song_name, 
                       match_found, kag_index, spot_index, match_score, 
                       artist_match_pct, name_match_pct):
    results.append({
        'spot_artists': spot_artists,
        'spot_song_name': spot_song_name,
        'kag_artists': kag_artists,
        'kag_song_name': kag_song_name,
        'match_found': match_found,
        'kag_index': kag_index,
        'spot_index': spot_index,
        'best_match_score': match_score,
        'best_match_artist_match_pct': artist_match_pct,
        'best_match_name_match_pct': name_match_pct
    })
    

df_spot = pd.read_csv('data/spotify_tracks_cleaned_20250418_223654.csv', encoding='utf-8')
df_kag = pd.read_csv('data/unique_hot100_20250418_222928.csv', encoding='utf-8')

df_kag = df_kag[df_kag['peak_chart_pos'] <= 10]
# Reset index after filtering to avoid key errors
df_kag = df_kag.reset_index(drop=True)
print(f"Filtered Kaggle dataset to {len(df_kag)} records with peak chart position <= 10")

df_kag['has_spot_match'] = False
df_kag['spot_match_index'] = None
df_spot['has_kag_match'] = False
df_spot['kag_match_index'] = None

results = []

start_time = time.time()

kag_performers = set(df_kag['artists'].unique())
performer_indices = {performer: df_kag[df_kag['artists'] == performer].index[0] 
                     for performer in kag_performers}

performer_to_titles = {}
for idx, row in df_kag.iterrows():
    performer = row['artists']
    title = row['song_name']
    if performer not in performer_to_titles:
        performer_to_titles[performer] = set()
    performer_to_titles[performer].add(title)

kag_performer_title_to_index = {}
for idx, row in df_kag.iterrows():
    performer = row['artists']
    title = row['song_name']
    kag_performer_title_to_index[(performer, title)] = idx

# Pre-compute word sets
kag_artist_words_dict = {idx: set(row['artists'].split()) for idx, row in df_kag.iterrows()}
kag_title_words_dict = {idx: set(row['song_name'].split()) for idx, row in df_kag.iterrows()}

# Create artist+first letter of title index
artist_title_first = {}
for idx, row in df_kag.iterrows():
    if row['song_name']:
        key = (row['artists'], row['song_name'][0])
        if key not in artist_title_first:
            artist_title_first[key] = []
        artist_title_first[key].append(idx)

for index, row in df_spot.iterrows():
    if df_spot.at[index, 'has_kag_match']:
        continue

    artist = row['artists']
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
            
            # Add to results with perfect scores
            match_found = True
            append_match_result(results, artist, song_name, curr_performer, song_name, 
                                match_found, best_match_kag_index, index, 100, 100, 100)
            continue

        start_index = performer_indices[curr_performer]

        for i in range(start_index, len(df_kag)):

            if df_kag.at[i, 'has_spot_match']:
                continue

            curr_performer = df_kag.iloc[i]['artists']
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

                curr_performer = df_kag.iloc[i]['artists']
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

            curr_performer = df_kag.iloc[i]['artists']
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



end_time = time.time()
elapsed_time = end_time - start_time
print(f"\nTotal time taken for processing: {elapsed_time:.2f} seconds")


results_df = pd.DataFrame(results)
results_df.sort_values(by=['best_match_score'], inplace=True)
results_df.reset_index(drop=True, inplace=True)

# Create and assign common track IDs
print("Creating common track IDs across datasets...")

# Add track_common_id column to both dataframes
df_spot['track_common_id'] = None
df_kag['track_common_id'] = None

# Generate UUID-like unique IDs
track_id_counter = 1
track_id_mapping = {}  # To track which tracks have already been assigned IDs

# First, process matches - they should share the same ID
for _, row in results_df.iterrows():
    if row['match_found']:
        spot_idx = row['spot_index']
        kag_idx = row['kag_index']
        
        # Generate a unique ID if neither track has one yet
        track_id = f"TRACK_{track_id_counter:06d}"
        track_id_counter += 1
        
        # Assign the same ID to both the Spotify and Kaggle tracks
        df_spot.at[spot_idx, 'track_common_id'] = track_id
        df_kag.at[kag_idx, 'track_common_id'] = track_id
        
        # Remember this assignment
        track_id_mapping[('spotify', spot_idx)] = track_id
        track_id_mapping[('kaggle', kag_idx)] = track_id

# Then, handle any unmatched tracks
for idx in range(len(df_spot)):
    if df_spot.at[idx, 'track_common_id'] is None:
        track_id = f"TRACK_{track_id_counter:06d}"
        track_id_counter += 1
        df_spot.at[idx, 'track_common_id'] = track_id
        track_id_mapping[('spotify', idx)] = track_id

for idx in range(len(df_kag)):
    if df_kag.at[idx, 'track_common_id'] is None:
        track_id = f"TRACK_{track_id_counter:06d}"
        track_id_counter += 1
        df_kag.at[idx, 'track_common_id'] = track_id
        track_id_mapping[('kaggle', idx)] = track_id

# Save this mapping for future use with Last.fm
mapping_df = pd.DataFrame([
    {'source': source, 'source_index': idx, 'track_common_id': track_id}
    for (source, idx), track_id in track_id_mapping.items()
])

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
match_results_file_name = f"match_results_{timestamp}.csv"
mapping_file_name = f"track_id_mapping_{timestamp}.csv"
spot_file_name = f"spotify_with_common_ids_{timestamp}.csv"
kag_file_name = f"kaggle_with_common_ids_{timestamp}.csv"

data_dir = os.path.join(os.getcwd(), 'data')
os.makedirs(data_dir, exist_ok=True)

# Save all files
match_results_file_path = os.path.join(data_dir, match_results_file_name)
mapping_file_path = os.path.join(data_dir, mapping_file_name)
spot_file_path = os.path.join(data_dir, spot_file_name)
kag_file_path = os.path.join(data_dir, kag_file_name)

results_df.to_csv(match_results_file_path, index=False, encoding='utf-8-sig')
mapping_df.to_csv(mapping_file_path, index=False, encoding='utf-8-sig')
df_spot.to_csv(spot_file_path, index=False, encoding='utf-8-sig')
df_kag.to_csv(kag_file_path, index=False, encoding='utf-8-sig')

print(f"Successfully wrote to {match_results_file_name}\n")
print(f"Created and saved track ID mapping to {mapping_file_name}")
print(f"Saved Spotify tracks with common IDs to {spot_file_name}")
print(f"Saved Kaggle tracks with common IDs to {kag_file_name}")
print(f"Generated {track_id_counter-1} unique track IDs")





