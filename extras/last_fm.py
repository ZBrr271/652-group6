import requests
import time
import pandas as pd

lastfm_key = "785f1b57835250d3bec5b0c2682b32aa"
API_KEY = '785f1b57835250d3bec5b0c2682b32aa'
BASE_URL = "http://ws.audioscrobbler.com/2.0/"

import requests
import time

tracks_per_tag = 150
limit_per_page = 150
max_tags = 25


top_tags_url = f'http://ws.audioscrobbler.com/2.0/?method=tag.getTopTags&api_key={API_KEY}&format=json'
response = requests.get(top_tags_url)
top_tags_data = response.json()
top_tags = [tag['name'] for tag in top_tags_data['toptags']['tag']]

#top_tags = [(tag['name'], tag['count']) for tag in top_tags_data['toptags']['tag']]
#print("Top tags on Last.fm: \n")
#for i, tag in enumerate(top_tags_data['toptags']['tag'], 1):
    #print(f"{i}. {tag['name']}, {tag['count']}")
print()

def tag_get_top_tracks(tag):
    tag_tracks = []
    page = 1

    while len(tag_tracks) < tracks_per_tag:
        params = {
            "method": "tag.getTopTracks",
            "tag": tag,
            "api_key": API_KEY,
            "format": "json",
            "limit": limit_per_page,
            "page": page
        }

        response = requests.get(BASE_URL, params=params).json()
        request_url = f"{BASE_URL}?" + "&".join([f"{key}={value}" for key, value in params.items()])
        print(f"Request URL: {request_url}")

        tracks = response.get("tracks", {}).get("track", [])
        attr = response.get("tracks", {}).get("@attr", {})
        
        if not tracks:
            break

        tag_tracks.extend(tracks)
        print(f"{tag} – page {page}, total pages: {attr.get('totalPages')}, tracks returned: {len(tracks)}")

        page += 1
        time.sleep(0.25)

    return tag_tracks[:tracks_per_tag]


for tag in top_tags[:max_tags]:
    all_tracks = {}

    print(f"Fetching top {tracks_per_tag} tracks for tag: {tag}")
    tracks_data = tag_get_top_tracks(tag)
    print(f"Fetched {len(tracks_data)} tracks for tag: {tag}\n")

    all_tracks[tag] = tracks_data
    
    time.sleep(0.5)

print()

"""
for tag, tracks in all_tracks.items():
    print(f"\nTop 10 tracks for '{tag}':")
    for track in tracks[:10]:
        print(f"{track['name']} - {track['artist']['name']}")
"""

