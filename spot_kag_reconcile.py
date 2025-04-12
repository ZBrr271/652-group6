import pandas as pd
from rapidfuzz import process, fuzz
import numpy as np
import time
from datetime import datetime


df_spot = pd.read_csv('spot_cleaned_20250411_172050.csv', encoding='utf-8')
df_kag = pd.read_csv('unique_hot100_20250411_231713.csv', encoding='utf-8')


results = []

start_time = time.time()

for index, row in df_spot.iterrows():
    artist = row['artists']
    name = row['name']
    match_found = False

    best_match = None 
    best_match_artist = None
    best_match_name = None
    best_match_performer = None
    best_match_title = None
    best_match_artist_match_pct = 0
    best_match_name_match_pct = 0
    best_mean_score = 0

    if index % 10 == 0:
        print(f"Processing record {index + 1} of {len(df_spot)}...")

    if artist in df_kag['performer'].values:
        start_index = df_kag[df_kag['performer'] == artist].index[0]

        for i in range(start_index, len(df_kag)):
            performer = df_kag.iloc[i]['performer']
            title = df_kag.iloc[i]['title']

            artist_score = fuzz.token_sort_ratio(artist, performer)
            name_score = fuzz.token_sort_ratio(name, title)

            mean_score = (artist_score + name_score) / 2

            if mean_score > best_mean_score:
                best_mean_score = mean_score
                best_match_artist = artist
                best_match_name = name
                best_match_performer = performer
                best_match_title = title
                best_match_artist_match_pct = artist_score
                best_match_name_match_pct = name_score

            if mean_score >= 85:
                results.append({
                    'artists': artist,
                    'name': name,
                    'performer': performer,
                    'title': title,
                    'artist_match_pct': artist_score,
                    'name_match_pct': name_score,
                    'match_percentage': mean_score,
                    'best_mean_score': best_mean_score,
                    'best_match_artist': best_match_artist,
                    'best_match_name': best_match_name,
                    'best_match_performer': best_match_performer,
                    'best_match_title': best_match_title,
                    'best_match_artist_match_pct': best_match_artist_match_pct,
                    'best_match_name_match_pct': best_match_name_match_pct
                })
                match_found = True
                break  

        if not match_found:
            for i in range(0, start_index):
                performer = df_kag.iloc[i]['performer']
                title = df_kag.iloc[i]['title']

                artist_score = fuzz.token_sort_ratio(artist, performer)
                name_score = fuzz.token_sort_ratio(name, title)

                mean_score = (artist_score + name_score) / 2

                if mean_score > best_mean_score:
                    best_mean_score = mean_score
                    best_match_artist = artist
                    best_match_name = name
                    best_match_performer = performer
                    best_match_title = title
                    best_match_artist_match_pct = artist_score
                    best_match_name_match_pct = name_score

                if mean_score >= 85:
                    results.append({
                        'artists': artist,
                        'name': name,
                        'performer': performer,
                        'title': title,
                        'artist_match_pct': artist_score,
                        'name_match_pct': name_score,
                        'match_percentage': mean_score,
                        'best_mean_score': best_mean_score,
                        'best_match_artist': best_match_artist,
                        'best_match_name': best_match_name,
                        'best_match_performer': best_match_performer,
                        'best_match_title': best_match_title,
                        'best_match_artist_match_pct': best_match_artist_match_pct,
                        'best_match_name_match_pct': best_match_name_match_pct
                    })
                    match_found = True
                    break 

    else:
        for i in range(0, len(df_kag)):
            performer = df_kag.iloc[i]['performer']
            title = df_kag.iloc[i]['title']

            artist_score = fuzz.token_sort_ratio(artist, performer)
            name_score = fuzz.token_sort_ratio(name, title)

            mean_score = (artist_score + name_score) / 2

            if mean_score > best_mean_score:
                best_mean_score = mean_score
                best_match_artist = artist
                best_match_name = name
                best_match_performer = performer
                best_match_title = title
                best_match_artist_match_pct = artist_score
                best_match_name_match_pct = name_score

            if mean_score >= 85:
                results.append({
                    'artists': artist,
                    'name': name,
                    'performer': performer,
                    'title': title,
                    'artist_match_pct': artist_score,
                    'name_match_pct': name_score,
                    'match_percentage': mean_score,
                    'best_mean_score': best_mean_score,
                    'best_match_artist': best_match_artist,
                    'best_match_name': best_match_name,
                    'best_match_performer': best_match_performer,
                    'best_match_title': best_match_title,
                    'best_match_artist_match_pct': best_match_artist_match_pct,
                    'best_match_name_match_pct': best_match_name_match_pct
                })
                match_found = True
                break 

    
    if not match_found:
        if best_match_name_match_pct > 0 and best_match_artist_match_pct > 0:
            results.append({
                'artists': best_match_artist,
                'name': best_match_name,
                'performer': best_match_performer,
                'title': best_match_title,
                'artist_match_pct': best_match_artist_match_pct,
                'name_match_pct': best_match_name_match_pct,
                'match_percentage': best_mean_score,
                'best_mean_score': best_mean_score,
                'best_match_artist': best_match_artist,
                'best_match_name': best_match_name,
                'best_match_performer': best_match_performer,
                'best_match_title': best_match_title,
                'best_match_artist_match_pct': best_match_artist_match_pct,
                'best_match_name_match_pct': best_match_name_match_pct
            })
        else:
            results.append({
                'artists': artist,
                'name': name,
                'performer': None,
                'title': None,
                'artist_match_pct': 0,
                'name_match_pct': 0,
                'match_percentage': 0,
                'best_mean_score': best_mean_score,
                'best_match_artist': best_match_artist,
                'best_match_name': best_match_name,
                'best_match_performer': best_match_performer,
                'best_match_title': best_match_title,
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
