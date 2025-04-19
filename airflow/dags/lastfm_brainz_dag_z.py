from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests
import time
import json
from datetime import datetime, timedelta
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None
}

MAX_TAGS = 50
TRACKS_PER_TAG = 10
api_key = Variable.get("LASTFM_Z_KEY")
base_url = Variable.get("LASTFM_BASE_URL")
user_agent = Variable.get("LAST_FM_USER_AGENT")


def get_top_tags():
    print(f"Getting top {MAX_TAGS} tags from LastFM")
    
    # get postgres connection
    pg_hook = PostgresHook(postgres_conn_id='pg_group6')

    # set up params for lastfm api
    params = {
        "method": "tag.getTopTags",
        "api_key": api_key,
        "format": "json",
        "user_agent": user_agent
    }

    # call lastfm api
    top_tags_r = requests.get(base_url, params=params)
    top_tags_data = top_tags_r.json()
    df_top_tags = pd.DataFrame(top_tags_data['toptags']['tag'][:MAX_TAGS])
    
    # write data to postgres
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for index, row in df_top_tags.iterrows():
                cur.execute("""INSERT INTO lastfm_top_tags_z (tag_name, tag_count, tag_reach) 
                            VALUES (%s, %s, %s)""", (row['name'], row['count'], row['reach']))
        conn.commit()
    print("Saved tags to database")
    return


def get_track_basics():
    print(f"Getting basics for {TRACKS_PER_TAG} tracks for each tag")
    
    # get api key and base url from airflow variables
    api_key = Variable.get("LASTFM_Z_KEY")
    base_url = Variable.get("LASTFM_BASE_URL")
    user_agent = Variable.get("LAST_FM_USER_AGENT")

    # get postgres connection
    pg_hook = PostgresHook(postgres_conn_id='pg_group6')



    # get top tags from db
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tag_name FROM lastfm_top_tags_z")
            top_tags = [tag[0] for tag in cur.fetchall()]


    all_tracks_initial = []
    track_data = []

    # get tracks for each tag
    for tag in top_tags:
        print(f"Getting tracks for tag: {tag}")
        tag_tracks = []
        page = 1
        while len(tag_tracks) < TRACKS_PER_TAG:
            time.sleep(0.2)
            params = {
                "method": "tag.getTopTracks",
                "tag": tag,
                "api_key": api_key,
                "format": "json",
                "limit": TRACKS_PER_TAG,
                "user_agent": user_agent,
                "page": page
            }

            response = requests.get(base_url, params=params).json()
            tracks = response.get("tracks", {}).get("track", [])
            if not tracks:
                break

            for track in tracks:
                track["tag"] = tag

            tag_tracks.extend(tracks)
            page += 1
        tag_tracks = tag_tracks[:TRACKS_PER_TAG]
        all_tracks_initial.extend(tag_tracks)
    
    print("Total number of tracks retrieved from LastFM:", len(all_tracks_initial))
    consolidated_tracks = {}
    for track in all_tracks_initial:
        mbid = track['mbid']
        if not mbid:
            continue
        tag = track.get('tag', None)
        rank = track.get('@attr', {}).get('rank', None)
        if mbid in consolidated_tracks:
            consolidated_tracks[mbid]['tag_ranks'][tag] = rank
        else:
            track_basics = {
                'mbid': mbid,
                'tag_ranks': {tag: rank}
            }
            consolidated_tracks[mbid] = track_basics
    
    track_data = list(consolidated_tracks.values())
    df_lastfm_initial = pd.DataFrame(track_data)
    initial_count_raw = len(all_tracks_initial)
    final_count = len(df_lastfm_initial)
    print(f"Retrieved basic info for {initial_count_raw} raw tracks")
    print(f"After consolidating tracks by 'mbid', we have {final_count} unique tracks")
    print(f"Removed {initial_count_raw - final_count} duplicate tracks")

    return df_lastfm_initial


def get_track_details(df_initial):
    request_count = 0
    all_tracks_details = []

    for index, row in df_initial.iterrows():
        mbid = row['mbid']

        params = {
            "method": "track.getInfo",
            "mbid": mbid,
            "api_key": api_key,
            "format": "json",
            "user_agent": user_agent
        }

        request_count += 1
        try:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                response_data = response.json()
                if 'track' in response_data:
                    track_data = response_data['track']
                    track_data['tag_ranks'] = row['tag_ranks']
                    all_tracks_details.append(track_data)
                if request_count % 10 == 0:
                    print(f"Retrieved last.fm track info for {request_count} tracks...")
            else:
                print(f"Failed to retrieve track info for {mbid}. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error retrieving track info for {mbid}: {e}")
    
        time.sleep(0.2)

    return all_tracks_details


def process_lastfm_tracks(all_tracks_details):
    print("Processing last.fm tracks...")
    lastfm_tracks = []
    for track in all_tracks_details:
        track_details = {}
        track_details['artist'] = track.get('artist', {}).get('name', None)
        track_details['song_name'] = track.get('name', None)
        track_details['duration'] = track.get('duration', None)
        track_details['listeners'] = track.get('listeners', None)
        track_details['playcount'] = track.get('playcount', None)
        track_details['mbid'] = track.get('mbid', None)
        track_details['album_name'] = track.get('album', {}).get('title', None)
        track_details['url'] = track.get('url', None)
        track_details['tag_ranks'] = json.dumps(track.get('tag_ranks', None))
        toptags = track.get('toptags', {}).get('tag', [])
        if toptags:
            tag_names = [tag.get('name') for tag in toptags if tag.get('name')]
            track_details['toptags'] = tag_names if tag_names else None
        else:
            track_details['toptags'] = None
        track_details['wiki_summary'] = track.get('wiki', {}).get('summary', None)

        lastfm_tracks.append(track_details)

    df_lastfm_tracks = pd.DataFrame(lastfm_tracks)

    df_lastfm_tracks.replace('', None, inplace=True)

    cols_to_clean = ['artist', 'song_name', 'album_name']
    for col in cols_to_clean:
        if col in df_lastfm_tracks.columns:
            df_lastfm_tracks[col] = df_lastfm_tracks[col].str.lower().str.strip()

    # Make sure integer columns are ints
    int_columns = ['duration', 'listeners', 'playcount']    
    for col in int_columns:
        df_lastfm_tracks[col] = pd.to_numeric(df_lastfm_tracks[col], errors='coerce')
    df_lastfm_tracks[int_columns] = df_lastfm_tracks[int_columns].where(pd.notnull(df_lastfm_tracks[int_columns]), None)
    for col in int_columns:
        df_lastfm_tracks[col] = df_lastfm_tracks[col].astype('Int64')

    print(f"Processed and cleaned {len(df_lastfm_tracks)} tracks")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    data_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(data_dir, exist_ok=True)
    lastfm_clean_file_name = f"lastfm_clean_{timestamp}.csv"
    lastfm_clean_file_path = os.path.join(data_dir, lastfm_clean_file_name)
    df_lastfm_tracks.to_csv(lastfm_clean_file_path, index=False, encoding='utf-8-sig')
    print(f"Successfully processed Last.fm tracks...")
    print(f"Successfully wrote record to {lastfm_clean_file_name}\n")

    return df_lastfm_tracks


def get_and_load_lastfm_tracks():
    print("Getting and loading last.fm tracks...")
    df_initial = get_track_basics()
    all_tracks_details = get_track_details(df_initial)
    df_lastfm_tracks = process_lastfm_tracks(all_tracks_details)
    
    # get postgres connection
    pg_hook = PostgresHook(postgres_conn_id='pg_group6')
    
    # write data to postgres
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for index, row in df_lastfm_tracks.iterrows():
                cur.execute("""INSERT INTO lastfm_tracks_z (artist, song_name, duration, listeners, 
                            playcount, mbid, album_name, url, tag_ranks, toptags, wiki_summary) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                            (row['artist'], row['song_name'], row['duration'], row['listeners'], 
                             row['playcount'], row['mbid'], row['album_name'], row['url'], 
                             row['tag_ranks'], row['toptags'], row['wiki_summary']))
            conn.commit()
            print(f"Saved {len(df_lastfm_tracks)} tracks to postgres")

    return    


with DAG(
        'lastfm_brainz_dag_z',
        default_args=default_args,
        description='Dag for lastfm and AcousticBrainz data',
        schedule_interval=None,
        catchup=False
    ) as lastfm_dag:
    
    start_task = EmptyOperator(task_id='start')

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='pg_group6',
        sql='sql/lastfm_z_testbed.sql'
    )

    load_top_tags = PythonOperator(
        task_id='load_top_tags',
        python_callable=get_top_tags
    ) 

    load_lastfm_tracks = PythonOperator(   
        task_id='load_lastfm_tracks',
        python_callable=get_and_load_lastfm_tracks
    )

    end_task = EmptyOperator(task_id='end')
    
    start_task >> create_tables >> load_top_tags >> load_lastfm_tracks >> end_task

