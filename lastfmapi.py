import requests
import time
import pandas as pd

# List of ISRCs

isrc_list = [
    "USSM19400325",
    "USMC15848998",
    "USMC15746480",
    "USMC16414508",
    "USSM12105970",
    "USSM16300086",
    "GBBBM8400019",
    "USRC17002573",
    "USSM12103949",
    "USSM10905166"
]




# MusicBrainz API URL for fetching data using ISRC
musicbrainz_url = "https://musicbrainz.org/ws/2/recording/"
lastfm_key = "785f1b57835250d3bec5b0c2682b32aa"


mbid_list = []
mbid_list2 = ['866dbbb3-f536-45d7-8bea-4077297a859f',
              'f980fc14-e29b-481d-ad3a-5ed9b4ab6340',
              '0ebe2d92-a11d-4b2b-9922-806383074ed7',
              '8e938046-d773-4d61-991a-0c8694bd0325',
              'e76c6012-f981-4f2f-bf64-dc7c0f3d0ec0',
              '9e3bb7c5-5704-4472-b1ba-dfd5a5f16c09',
              'c23e6045-09d3-4936-a3ed-3c122e02d76e',
              '0fd07598-5881-47ba-9eca-5a7378357c2b',
              'b7c17cf1-15cb-446b-9194-6bcdd5e9041a',
              '9abaaa73-fa68-4e03-9edc-d1992d9825d4']


def fetch_lastfm_data(mbid2):

    url = f"http://ws.audioscrobbler.com/2.0/?method=track.getInfo&mbid={mbid2}&api_key={lastfm_key}&format=json"
    response = requests.get(url).json()
    time.sleep(1)
    if "track" in response:
        track_info = response["track"]
        return {
            "track": track_info["name"],
            "artist": track_info["artist"]["name"],
            "duration": int(track_info["duration"]),
            "toptag": track_info["toptags"]["tag"][0]["name"] if track_info["toptags"]["tag"] else None,
            "listeners": int(track_info["listeners"]),
            "playcount": int(track_info["playcount"]),
        }
    else:
        print(f"Failed to fetch data for MBID: {mbid2}")
        print("Response from last.fm: ", response)
    return None

def extract_all_tracks():
    track_data = [fetch_lastfm_data(mbid) for mbid in mbid_list2]
    return [track for track in track_data if track]  # Filter out failed requests


# Function to fetch data from MusicBrainz using ISRC
def get_track_data_from_musicbrainz(isrc):
    params = {
        "query": f"isrc:{isrc}",
        "fmt": "json"
    }
    
    response = requests.get(musicbrainz_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if data["recordings"]:
            # Assuming the first result is the correct one
            track_info = data["recordings"][0]
            return track_info
    return None

# Loop through the list of ISRCs and fetch data for each
for isrc in isrc_list:
    print(f"Fetching data for ISRC: {isrc}")
    
    # Step 1: Get track data from MusicBrainz using ISRC
    track_data = get_track_data_from_musicbrainz(isrc)
    
    if track_data:
        print(f"Track found: {track_data['title']}")
        mbid = track_data['id']
        print(f"MBID: {mbid}")
        mbid_list.append(mbid)
        #print(track_data)
        # Extract and print some relevant information
        #artists = ", ".join([artist['name'] for artist in track_data['artists']]) if 'artists' in track_data else "Unknown"
        #release_date = track_data.get("first-release-date", "Unknown")
        #genre = "Unknown"  # MusicBrainz doesn't directly return genre for recordings
        #album_title = track_data.get("release-group", {}).get("title", "Unknown")
        
        #print(f"Artist(s): {artists}")
        #print(f"Album: {album_title}")
        #print(f"Release Date: {release_date}")
        #print("-" * 30)
    else:
        print(f"No track found for ISRC: {isrc}")

    time.sleep(1)
    print()


print(mbid_list)
print(len(mbid_list))
print()

lastfm_data_list = extract_all_tracks()
print(lastfm_data_list)
print(len(lastfm_data_list))

df = pd.DataFrame(lastfm_data_list)
print(df.to_string(index=False))


