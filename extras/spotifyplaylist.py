import requests
import base64
from urllib.parse import urlencode
import time
import pandas as pd
import copy
from datetime import datetime

client_id = 'd2d5bf12b79f4947be8568a2a4105a93'
client_secret = '61ec80164ca14449a0588e4e64330fda'

# Encode client_id and client_secret in Base64
auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

# Set request headers
headers1 = {
    "Authorization": f"Basic {auth_header}",
    "Content-Type": "application/x-www-form-urlencoded",
}

# Set request payload
data = {
    "grant_type": "client_credentials"
}

# Send POST request
response = requests.post("https://accounts.spotify.com/api/token", headers=headers1, data=data)

# Handle response
if response.status_code == 200:
    token = response.json().get("access_token")
    print("Access Token:", token)
    tokenType = response.json().get("token_type")
    print("Token Type: ", tokenType)
    expiresIn = response.json().get("expires_in")
    print("Expires In: ", expiresIn)
else:
    print("Error:", response.status_code, response.text)
print()

# Set the base URLs and search parameters
search_url = "https://api.spotify.com/v1/search"

headers = {"Authorization": f"Bearer {token}"}


# A couple good search queries are below
# Top 1000 greatest songs of all time - 3Q1DIJ51dJpUO6RhnIHdVx
# Billboard Hot 100: All #1 hit songs 1958-2024 - 7c2c13pKxvFDSV4WSyydyg

playlist_id = "7c2c13pKxvFDSV4WSyydyg"

# Get tracks from the playlist (handling pagination)
tracks_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
track_data = []
all_track_details = []
track_count = 0

while tracks_url:
    time.sleep(1)
    response = requests.get(tracks_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        track_items = data.get("items", [])

        for track_info in track_items:
            track = track_info.get("track", {})
            track_name = track.get("name", "Unknown Track")
            track_id = track.get("id", "No ID")
            artist_name = ", ".join([artist["name"] for artist in track.get("artists", [])])
            duration_ms = track.get("duration_ms", 0)

            track_data.append({
                "Track Name": track_name,
                "Track ID": track_id,
                "Artist(s)": artist_name,
                "Duration (ms)": duration_ms
            })

            track_details = {key: track.get(key, "Unknown") for key in track.keys()}

            track_details.pop("preview_url", None)

            artists = track.get("artists", [])
            artist_names = [artist.get("name", "Unknown Artist") for artist in artists]
            track_details["artists"] = ", ".join(artist_names)

            track_details["available_markets"] = ", ".join(track.get("available_markets", []))

            album = track.get("album", {})
            for album_key, album_value in album.items():
                col_name = f"album_{album_key}"
                if isinstance(album_value, list):
                    if all(isinstance(i, dict) for i in album_value):
                        track_details[col_name] = "; ".join(
                            [str({k: v for k, v in item.items()}) for item in album_value]
                        )
                    else:
                        track_details[col_name] = ", ".join(map(str, album_value))

                elif isinstance(album_value, dict):
                    # Flatten dicts as a string
                    track_details[col_name] = ", ".join(f"{k}: {v}" for k, v in album_value.items())

                else:
                    # Keep simple values
                    track_details[col_name] = album_value
            track_details.pop("album", None)

            track_details["external_id_isrc"] = track.get("external_ids", {}).get("isrc", "")
            track_details.pop("external_ids", None)

            track_details["spotify_url"] = track.get("external_urls", {}).get("spotify", "")
            track_details.pop("external_urls", None)


            all_track_details.append(track_details)

            track_count += 1
            print(f"{track_count}. {track_name} - {artist_name} (Duration: {duration_ms} ms)")

        tracks_url = data.get("next")

    else:
        print("Error fetching tracks:", response.status_code)
        break

print(f"\nTotal Tracks Retrieved: {track_count}")

df = pd.DataFrame(all_track_details)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
spot_file = f"spot_tracks_{timestamp}.csv"
df.to_csv(spot_file, index=False)

spot_df = df.copy()

cols_to_transform = ['artists', 'name', 'album_name']

for col in cols_to_transform:
    if col in spot_df.columns:
        spot_df[col] = spot_df[col].str.lower().str.strip()

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
spot_cleaned = f"spot_cleaned_{timestamp}.csv"
spot_df.to_csv(spot_cleaned, index=False)


