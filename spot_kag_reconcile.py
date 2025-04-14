import pandas as pd
from rapidfuzz import process, fuzz
import numpy as np
import time
from datetime import datetime
from math import sqrt


df_spot = pd.read_csv('spot_tracks_20250413_023044.csv', encoding='utf-8')
df_kag = pd.read_csv('unique_hot100_20250412_212924.csv', encoding='utf-8')

results = []

kag_performers = set(df_kag['performer'].unique())
performer_indices = {performer: df_kag[df_kag['performer'] == performer].index[0] 
                     for performer in kag_performers}

start_time = time.time()

for index, row in df_spot.iterrows():
    artist = row['artists']
    name = row['name']
    match_found = False

    best_match_performer = None
    best_match_title = None
    best_match_score = 0
    best_match_artist_match_pct = 0
    best_match_name_match_pct = 0

    if index % 10 == 0:
        print(f"Processing record {index + 1} of {len(df_spot)}...")

    if artist in kag_performers:
        performer = artist
        start_index = performer_indices[performer]

        for i in range(start_index, len(df_kag)):
            title = df_kag.iloc[i]['title']

            artist_score = fuzz.token_sort_ratio(artist, performer)
            name_score = fuzz.partial_ratio(name, title)
            #name_score = fuzz.token_sort_ratio(name, title)

            mean_score = (artist_score + name_score) / 2

            if mean_score > best_match_score:
                best_match_performer = performer
                best_match_title = title
                best_match_score = mean_score
                best_match_artist_match_pct = artist_score
                best_match_name_match_pct = name_score

            if mean_score >= 85:
                match_found = True
                results.append({
                    'artists': artist,
                    'name': name,
                    'best_match_performer': best_match_performer,
                    'best_match_title': best_match_title,
                    'best_match_score': best_match_score,
                    'best_match_artist_match_pct': best_match_artist_match_pct,
                    'best_match_name_match_pct': best_match_name_match_pct
                })
                break  

        if not match_found:
            for i in range(0, start_index):
                title = df_kag.iloc[i]['title']

                artist_score = fuzz.token_sort_ratio(artist, performer)
                name_score = fuzz.partial_ratio(name, title)
                #name_score = fuzz.token_sort_ratio(name, title)

                mean_score = (artist_score + name_score) / 2

                if mean_score > best_match_score:
                    best_match_performer = performer
                    best_match_title = title
                    best_match_score = mean_score
                    best_match_artist_match_pct = artist_score
                    best_match_name_match_pct = name_score

                if mean_score >= 85:
                    match_found = True
                    results.append({
                        'artists': artist,
                        'name': name,
                        'best_match_performer': best_match_performer,
                        'best_match_title': best_match_title,
                        'best_match_score': best_match_score,
                        'best_match_artist_match_pct': best_match_artist_match_pct,
                        'best_match_name_match_pct': best_match_name_match_pct
                    })
                    break 

    else:
        for i in range(0, len(df_kag)):
            performer = df_kag.iloc[i]['performer']
            title = df_kag.iloc[i]['title']

            artist_score = fuzz.token_sort_ratio(artist, performer)
            name_score = fuzz.partial_ratio(name, title)
            #name_score = fuzz.token_sort_ratio(name, title)

            mean_score = (artist_score + name_score) / 2

            if mean_score > best_match_score:
                best_match_performer = performer
                best_match_title = title
                best_match_score = mean_score
                best_match_artist_match_pct = artist_score
                best_match_name_match_pct = name_score

            if mean_score >= 85:
                match_found = True
                results.append({
                    'artists': artist,
                    'name': name,
                    'best_match_performer': best_match_performer,
                    'best_match_title': best_match_title,
                    'best_match_score': best_match_score,
                    'best_match_artist_match_pct': best_match_artist_match_pct,
                    'best_match_name_match_pct': best_match_name_match_pct
                })
                break 

    
    if not match_found:
        if best_match_name_match_pct > 0 and best_match_artist_match_pct > 0:
            results.append({
                'artists': artist,
                'name': name,
                'best_match_performer': best_match_performer,
                'best_match_title': best_match_title,
                'best_match_score': best_match_score,
                'best_match_artist_match_pct': best_match_artist_match_pct,
                'best_match_name_match_pct': best_match_name_match_pct
            })
        else:
            # This is the kind of record match_found is important for
            results.append({
                'artists': artist,
                'name': name,
                'best_match_performer': best_match_performer,
                'best_match_title': best_match_title,
                'best_match_score': best_match_score,
                'best_match_artist_match_pct': best_match_artist_match_pct,
                'best_match_name_match_pct': best_match_name_match_pct
            })

end_time = time.time()
elapsed_time = end_time - start_time
print(f"\nTotal time taken for processing: {elapsed_time:.2f} seconds")


results_df = pd.DataFrame(results)
print("\nResults DataFrame:")
print(results_df)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
match_test = f"match_test_{timestamp}.csv"
results_df.to_csv(match_test, index=False)
