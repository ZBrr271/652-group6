from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

MAX_TAGS = 10
MAX_TRACKS = 1000

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
    top_tags_r = requests.get(base_url, params=params)
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
                tracks_r = requests.get(base_url, params=params)
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
                
                print(f"Got {len(tracks_df)} tracks for tag: {tag}")

                # write data to postgres
                for index, row in tracks_df.iterrows():
                    cur.execute("""INSERT INTO lastfm_top_tag_tracks (track_name, track_mbid, artist_name, artist_mbid, tag, rank) 
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


with DAG(
        'lastfm_dag',
        default_args=default_args,
        description='Dag for lastfm data',
        schedule_interval=None,
        catchup=False
    ) as lastfm_dag:
    
    start_task = EmptyOperator(task_id='start')

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='pg_group6',
        sql='sql/lastfm_create.sql'
    )

    load_top_tags = PythonOperator(
        task_id='load_top_tags',
        python_callable=get_top_tags
    ) 

    load_tag_tracks = PythonOperator(   
        task_id='load_tag_tracks',
        python_callable=get_tag_tracks
    )

    end_task = EmptyOperator(task_id='end')
    
    start_task >> create_tables >> load_top_tags >> load_tag_tracks >> end_task

