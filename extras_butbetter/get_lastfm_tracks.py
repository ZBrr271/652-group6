# 685.652 - Group 6
# get_lastfm_tracks.py
# Get top tracks from top tags from last.fm

import requests
import time
import pandas as pd
from datetime import datetime
import os


API_KEY = '785f1b57835250d3bec5b0c2682b32aa'
BASE_URL = "http://ws.audioscrobbler.com/2.0/"
USER_AGENT = "Zach B - Just learning - zbarret1@jh.edu - always delays between requests"

# Arbitrary reasonable limits
tracks_per_tag = 100
limit_per_page = tracks_per_tag
max_tags = 25

data_dir = os.path.join(os.getcwd(), 'data')
os.makedirs(data_dir, exist_ok=True)

# Gets the most used tags on last.fm
def get_top_tags():
    params = {
        "method": "tag.getTopTags",
        "api_key": API_KEY,
        "format": "json",
        "user_agent": USER_AGENT
    }
    response = requests.get(BASE_URL, params=params)
    top_tags_data = response.json()
    df_top_tags = pd.DataFrame(top_tags_data['toptags']['tag'][:max_tags])
    df_top_tags.head()
    return df_top_tags


# Gets the most-tagged tracks for a given tag
def tag_get_top_tracks(tag):
    tag_tracks = []
    page = 1

    while len(tag_tracks) < tracks_per_tag:
        time.sleep(0.21)
        params = {
            "method": "tag.getTopTracks",
            "tag": tag,
            "api_key": API_KEY,
            "format": "json",
            "limit": limit_per_page,
            "page": page
        }

        response = requests.get(BASE_URL, params=params).json()
        tracks = response.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            track['tag'] = tag

        tag_tracks.extend(tracks)
        page += 1

    return tag_tracks[:tracks_per_tag]


def get_track_basics():
    all_tracks_initial = []
    track_data = []

    df_toptags = get_top_tags()
    top_tag_names = df_toptags['name'].tolist()
    print(f"Top tags: {top_tag_names}")

    for tag in top_tag_names[:max_tags]:
        tracks_data = tag_get_top_tracks(tag)
        print(f"Fetched top {len(tracks_data)} tracks for tag: {tag}")
        all_tracks_initial.extend(tracks_data)

    print(f"Total number of tracks retrieved: {len(all_tracks_initial)}")

    consolidated_tracks = {}

    for track in all_tracks_initial:
        mbid = track.get('mbid', None)
        
        if not mbid:
            continue
            
        tag = track.get('tag', None)
        rank = track.get('@attr', {}).get('rank', 'unknown')
        

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

    # Print results
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
        track_details = {}
        mbid = row['mbid']

        params = {
            "method": "track.getInfo",
            "api_key": API_KEY,
            "mbid": mbid,
            "format": "json",
            "user_agent": USER_AGENT
        }
        
        # Track request count
        request_count += 1
        
        try:
            response = requests.get(BASE_URL, params=params)
            if response.status_code == 200:
                response_data = response.json()
                if 'track' in response_data:
                    track_data = response_data['track']
                    track_data['tag_ranks'] = row['tag_ranks']
                    all_tracks_details.append(track_data)
                if request_count % 10 == 0:
                    print(f"Retrieved last.fm track info for {request_count} tracks...")
            else:
                print(f"Request failed for MBID {mbid}: Status code {response.status_code}")
        except Exception as e:
            print(f"Error processing MBID {mbid}: {str(e)}")
        
        time.sleep(0.21)

    print(f"Retrieved detailed info from last.fm for {len(all_tracks_details)} tracks\n")

    return all_tracks_details




def process_lastfm_tracks(all_tracks_details):
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
        track_details['tag_ranks'] = track.get('tag_ranks', None)
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


    return df_lastfm_tracks


df_initial = get_track_basics()
print(df_initial.head())

all_tracks_details = get_track_details(df_initial)
lastfm_tracks = process_lastfm_tracks(all_tracks_details)
print(lastfm_tracks.head())


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
lastfm_clean_file_name = f"lastfm_clean_{timestamp}.csv"
lastfm_clean_file_path = os.path.join(data_dir, lastfm_clean_file_name)
lastfm_tracks.to_csv(lastfm_clean_file_path, index=False, encoding='utf-8-sig')
print(f"Successfully processed Last.fm tracks...")
print(f"Successfully wrote record to {lastfm_clean_file_name}\n")



