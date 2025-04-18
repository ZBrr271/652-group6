# 685.652 - Group 6
# process_kaggle_file.py
# Clean data and filter duplicates


import pandas as pd
import os
from datetime import datetime
from tabulate import tabulate

def process_kag_file(file_name):
    print(f"\nProcessing Kaggle file: {file_name} ...\n"
          "File contains Bilboard Hot 100 chart data from 1958 to 2024\n"
          "It contains - chart_week, title, performer, current_week, last_week, peak_pos, wks_on_chart\n"
          "Duplicate song title, performer pairs will be removed\n")

    data_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(data_dir, exist_ok=True)
    kaggle_file_path = os.path.join(data_dir, file_name)

    # Read kaggle file from data directory
    df = pd.read_csv(kaggle_file_path, encoding='utf-8')

    # Strip and lowercase string cols
    df['title'] = df['title'].str.strip().str.lower()
    df['performer'] = df['performer'].str.strip().str.lower()

    # Force chart_wk to date for sorting
    df['chart_week'] = pd.to_datetime(df['chart_week'], errors='coerce')

    # Replace empty strings with None
    df.replace('', None, inplace=True)

    df_kaggle = df.copy()

    # Replace some strings to help with matching to Spotify data
    df_kaggle['performer'] = df_kaggle['performer'].str.replace(r' \(featuring ([^)]*)\)', r', \1', regex=True)
    df_kaggle['performer'] = df_kaggle['performer'].str.replace(r' \(feat. ([^)]*)\)', r', \1', regex=True)
    df_kaggle['performer'] = df_kaggle['performer'].str.replace('featuring', ',')
    df_kaggle['performer'] = df_kaggle['performer'].str.replace('feat.', ',')
    df_kaggle['performer'] = df_kaggle['performer'].str.replace('&', 'and')
    df_kaggle['title'] = df_kaggle['title'].str.replace(r' \(featuring.*$', '', regex=True)

    # Force int columns to numeric, force any NaNs to None
    int_columns = ['current_week', 'last_week', 'peak_pos', 'wks_on_chart']
    for col in int_columns:
        df_kaggle[col] = pd.to_numeric(df_kaggle[col], errors='coerce')
    df_kaggle[int_columns] = df_kaggle[int_columns].where(pd.notnull(df_kaggle[int_columns]), None)

    # Force int columns to Int64 (needed for one column that was being read as float)
    # Replace 0 with None
    for col in int_columns:
        df_kaggle[col] = df_kaggle[col].astype('Int64')
    for col in int_columns:
        df_kaggle[col] = df_kaggle[col].replace(0, None)


    print(f"Number of Kaggle dataset rows before removing duplicates: {len(df_kaggle)}")

    # Sort and remove duplicates, by equal performer and title
    # Keep most recent occurrence
    df_kaggle = df_kaggle.sort_values(by=['performer', 'title', 'chart_week'], 
                                ascending=[True, True, False]).reset_index(drop=True)

    df_kaggle = df_kaggle.drop_duplicates(subset=['title', 'performer'], 
                                          keep='first').reset_index(drop=True)

    print(f"Number of Kaggle dataset rows after removing duplicates: {len(df_kaggle)}\n")

    # This col not needed
    df_kaggle.drop(columns=['last_week'], inplace=True)

    # Rename columns
    df_kaggle.rename(columns={'chart_week': 'wk_last_charted',
                           'current_week': 'last_chart_pos',
                           'peak_pos': 'peak_chart_pos',
                           'wks_on_chart': 'total_wks_on_chart',
                           'performer': 'artists',
                           'title': 'song_name'}, inplace=True)
    
    # Reorder columns
    df_kaggle = df_kaggle[['artists', 'song_name', 'peak_chart_pos',
                           'last_chart_pos', 'total_wks_on_chart', 'wk_last_charted']]


    # Write to CSV for record
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_hot100 = f"unique_hot100_{timestamp}.csv"
    kag_file_name = os.path.join(data_dir, unique_hot100)

    df_kaggle.to_csv(kag_file_name, index=False,
                        encoding='utf-8-sig')
    print(f"Kaggle dataset processed and saved to {unique_hot100}\n")

    count = (df_kaggle['peak_chart_pos'] == 1).sum()
    print(f"Processed Kaggle dataset contains {len(df_kaggle)} songs in total and " 
          f"{count} songs that peaked at #1")
    print(f"Dataframe has columns: {', '.join(df_kaggle.columns)}\n")


    # Return the processed dataframe
    return df_kaggle


filename = 'hot-100-current.csv'
kag_name = process_kag_file(filename)
