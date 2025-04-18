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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None
}

MAX_TAGS = 10
MAX_TRACKS = 1000
HEADERS = {"User-Agent": "JHU-Project/1.0 (spalit2@jh.edu)"}

def get_top_tags():
    print(f"Getting top {MAX_TAGS} tags from LastFM")
    
    # get api key and base url from airflow variables
    api_key = Variable.get("LASTFM_API_KEY")
    base_url = Variable.get("LASTFM_BASE_URL")

    # get postgres connection
    pg_hook = PostgresHook(postgres_conn_id='pg_group6')

    # set up params for lastfm api
    params = {
        "method": "tag.getTopTags",
        "api_key": api_key,
        "format": "json",
    }

    # call lastfm api
    top_tags_r = requests.get(base_url, params=params, headers=HEADERS)
    top_tags_data = top_tags_r.json()
    tags_df = pd.DataFrame(top_tags_data['toptags']['tag'][:MAX_TAGS])
    print(f"Got tags: {tags_df['name'].tolist()}")
    
    # write data to postgres
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for index, row in tags_df.iterrows():
                cur.execute("""INSERT INTO lastfm_top_tags (tag_name, tag_count, tag_reach) 
                            VALUES (%s, %s, %s)""", (row['name'], row['count'], row['reach']))
        conn.commit()
    print("Saved tags to database")
    return


def get_tag_tracks():
    print(f"Getting {MAX_TRACKS} tracks for each tag")
    
    # get api key and base url from airflow variables
    api_key = Variable.get("LASTFM_API_KEY")
    base_url = Variable.get("LASTFM_BASE_URL")

    # get postgres connection
    pg_hook = PostgresHook(postgres_conn_id='pg_group6')

    # get top tags from db
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tag_name FROM lastfm_top_tags")
            top_tags = [tag[0] for tag in cur.fetchall()]

            # get tracks for each tag
            for tag in top_tags:
                print(f"Getting tracks for tag: {tag}")
                params = {
                    "method": "tag.getTopTracks",
                    "tag": tag,
                    "api_key": api_key,
                    "format": "json",
                    "limit": MAX_TRACKS,
                }
                
                # call lastfm api
                tracks_r = requests.get(base_url, params=params, headers=HEADERS)
                tracks_json = tracks_r.json()

                # update metadata
                for track in tracks_json['tracks']['track']:
                    track['tag'] = tag
                    track['rank'] = int(track['@attr']['rank'])
                    track['artist_name'] = track['artist']['name']
                    track['artist_mbid'] = track['artist']['mbid']

                # create dataframe
                tracks_df = pd.DataFrame(tracks_json['tracks']['track'])\
                            [['name','mbid','artist_name','artist_mbid','tag','rank']]\
                                .rename({'mbid':'track_mbid','name':'track_name'},axis=1)
                
                tracks_df['track_mbid'] = tracks_df['track_mbid'].replace('', None)
                tracks_df['artist_mbid'] = tracks_df['artist_mbid'].replace('', None)
                
                print(f"Got {len(tracks_df)} tracks for tag: {tag}")

                # write data to postgres
                for index, row in tracks_df.iterrows():
                    cur.execute("""INSERT INTO lastfm_top_tag_tracks (track_name, track_mbid, artist_name, artist_mbid, tag_name, tag_rank) 
                                VALUES (%s, %s, %s, %s, %s, %s)""", 
                                    (row['track_name'], 
                                     row['track_mbid'], 
                                     row['artist_name'], 
                                     row['artist_mbid'], 
                                     row['tag'], 
                                     row['rank']))
                conn.commit()
                print(f"Saved tracks for {tag}")

    return


def get_ab_features():
    print("Getting acousticbrainz features")

    # get base url from airflow variables
    base_url = Variable.get("AB_BASE_URL")
    endpoint = base_url + "/api/v1/high-level"

    # get postgres connection
    pg_hook = PostgresHook(postgres_conn_id='pg_group6')

    # get all mbid from lastfm_top_tag_tracks
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT distinct track_mbid FROM lastfm_top_tag_tracks WHERE track_mbid IS NOT NULL and track_mbid != ''")
            mbid_list = [mbid[0] for mbid in cur.fetchall()]

            batch_size = 25
            print('Starting to get features for', len(mbid_list), f'tracks using batch_size = {batch_size}')

            # get features for each batch of mbid
            for i in range(0, len(mbid_list), batch_size):
                batch = mbid_list[i:i + batch_size]
                params = {
                    'recording_ids': ';'.join(batch),
                }
                response = requests.get(endpoint, params=params, headers=HEADERS)

                # write batch to postgres
                for mbid, data in response.json().items():
                    if mbid not in batch:
                        continue
                    features = {
                        'mbid': mbid,
                        'isrcs': data['0']['metadata'].get('tags', {}).get('isrc', []),
                        'danceability': data['0']['highlevel'].get('danceability', {}).get('all', {}).get('danceable', None),
                        'genre_alternative': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('alternative',None),
                        'genre_blues': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('blues',None),
                        'genre_electronic': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('electronic',None),
                        'genre_folkcountry': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('folkcountry',None),
                        'genre_funksoulrnb': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('funksoulrnb',None),
                        'genre_jazz': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('jazz',None),
                        'genre_pop': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('pop',None),
                        'genre_raphiphop': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('raphiphop',None),
                        'genre_rock': data['0']['highlevel'].get('genre_dortmund', {}).get('all', {}).get('rock',None),
                        'mood_happy': data['0']['highlevel'].get('mood_happy', {}).get('all', {}).get('happy', None),
                        'mood_party': data['0']['highlevel'].get('mood_party', {}).get('all', {}).get('party', None),
                        'mood_relaxed': data['0']['highlevel'].get('mood_relaxed', {}).get('all', {}).get('relaxed', None),
                        'mood_sad': data['0']['highlevel'].get('mood_sad', {}).get('all', {}).get('sad', None),
                        'metadata': data['0']['metadata']['tags']
                    }

                    cur.execute("""INSERT INTO acousticbrainz_features (mbid, isrcs, danceability, genre_alternative, genre_blues, 
                                                                        genre_electronic, genre_folkcountry, genre_funksoulrnb, genre_jazz, 
                                                                        genre_pop, genre_raphiphop, genre_rock, mood_happy, mood_party, 
                                                                        mood_relaxed, mood_sad, metadata) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                                (features['mbid'], 
                                features['isrcs'], 
                                features['danceability'], 
                                features['genre_alternative'], 
                                features['genre_blues'], 
                                features['genre_electronic'], 
                                features['genre_folkcountry'], 
                                features['genre_funksoulrnb'], 
                                features['genre_jazz'], 
                                features['genre_pop'], 
                                features['genre_raphiphop'], 
                                features['genre_rock'], 
                                features['mood_happy'], 
                                features['mood_party'], 
                                features['mood_relaxed'], 
                                features['mood_sad'], 
                                json.dumps(features['metadata'])))
                
                conn.commit()
                
                print('Batch', i // batch_size + 1, 'of', len(mbid_list) // batch_size, 'comlpete')
                time.sleep(0.2)

                # for debugging
                # if (i // batch_size + 1) == 10:
                #     break
            
    return

with DAG(
        'lastfm_brainz_dag',
        default_args=default_args,
        description='Dag for lastfm data',
        schedule_interval=None,
        catchup=False
    ) as lastfm_dag:
    
    start_task = EmptyOperator(task_id='start')

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='pg_group6',
        sql='sql/lastfm_brainz_create.sql'
    )

    load_top_tags = PythonOperator(
        task_id='load_top_tags',
        python_callable=get_top_tags
    ) 

    load_tag_tracks = PythonOperator(   
        task_id='load_tag_tracks',
        python_callable=get_tag_tracks
    )

    load_ab_features = PythonOperator(
        task_id='load_ab_features',
        python_callable=get_ab_features
    )

    end_task = EmptyOperator(task_id='end')
    
    start_task >> create_tables >> load_top_tags >> load_tag_tracks >> load_ab_features >> end_task

