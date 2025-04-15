import requests
import time
import pandas as pd
from itertools import islice


API_KEY = '785f1b57835250d3bec5b0c2682b32aa'
BASE_URL = "http://ws.audioscrobbler.com/2.0/"

# Arbitrary reasonable limits
tracks_per_tag = 25
limit_per_page = tracks_per_tag
max_tags = 50

# Gets the most used tags on last.fm
def get_top_tags():
    params = {
        "method": "tag.getTopTags",
        "api_key": API_KEY,
        "format": "json",
    }
    response = requests.get(BASE_URL, params=params)
    return response.json()

top_tags_data = get_top_tags()
top_tags = [tag['name'] for tag in top_tags_data['toptags']['tag']]

# Gets the top tracks (up to tracks_per_tag) for a given tag
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

        tracks = response.get("tracks", {}).get("track", [])
        attr = response.get("tracks", {}).get("@attr", {})
        if not tracks:
            break

        for track in tracks:
            track['tag'] = tag  # Add tag as a key-value pair to each track

        tag_tracks.extend(tracks)
        page += 1

        # Do not remove this
        # It is the last_fm API's rate limit
        time.sleep(0.26)

    return tag_tracks[:tracks_per_tag]

all_tracks = []

for tag in top_tags[:max_tags]:
    print(f"Fetching top {tracks_per_tag} tracks for tag: {tag}")
    tracks_data = tag_get_top_tracks(tag)
    print(f"Fetched {len(tracks_data)} tracks for tag: {tag}\n")
    all_tracks.extend(tracks_data)
    
    time.sleep(0.6)


print("\nTop 5 tracks for each tag:\n")
for tag in top_tags[:max_tags]:
    # Filter tracks for the current tag
    tag_tracks = [track for track in all_tracks if track.get('tag') == tag]
    
    print(f"Tag: {tag} - Top 5 Tracks:")
    for i, track in enumerate(tag_tracks[:5], 1):
        print(f"  {i}. Track Name: {track['name']}, Artist: {track['artist']['name']}")
    print()

total_tracks = len(all_tracks)
print(f"Total number of tracks retrieved: {total_tracks}")


# Function to analyze track fields
def analyze_tracks(tracks, field_name="url"):
    total_tracks = len(tracks)
    tracks_with_field = sum(1 for track in tracks if track.get(field_name))
    unique_values = len(set(track.get(field_name) for track in tracks if track.get(field_name)))
    
    print(f"Total tracks: {total_tracks}")
    print(f"Tracks with {field_name}: {tracks_with_field}")
    print(f"Unique {field_name}s: {unique_values}")
    print()
    
    return tracks_with_field, unique_values


# First, add a function to initialize tag_ranks for all tracks
def initialize_tag_ranks(tracks):
    for track in tracks:
        # Create tag_ranks field using the tag and rank information
        tag = track.get('tag')
        rank = track.get('@attr', {}).get('rank', 'unknown')
        
        # Initialize tag_ranks as a string: "tag:rank"
        track['tag_ranks'] = f"{tag}:{rank}"
        
        # Remove the original tag field to avoid confusion
        if 'tag' in track:
            del track['tag']
    
    return tracks


# Updated function to handle both single and multiple fields for deduplication
def remove_duplicates_by_field(tracks, field_names):
    if not tracks:
        return []
        
    unique_tracks = []
    seen_values = {}
    
    # Ensure field_names is always a list
    if not isinstance(field_names, list):
        field_names = [field_names]
    
    for track in tracks:
        # For single field, get the value directly
        # For multiple fields, create a composite key
        if len(field_names) == 1:
            field_value = track.get(field_names[0])
        else:
            # Handle special case for artist name which is nested
            values = []
            for field in field_names:
                if field == 'artist_name':
                    # Extract artist name from nested structure
                    value = track.get('artist', {}).get('name', '')
                else:
                    value = track.get(field, '')
                
                # Convert to lowercase for case-insensitive comparison
                if isinstance(value, str):
                    value = value.lower()
                    
                values.append(str(value))
            
            # Create a composite key by joining the values
            field_value = '|'.join(values)
        
        # Skip tracks without all required fields
        if not field_value or (len(field_names) > 1 and '|' not in field_value):
            unique_tracks.append(track)
            continue
            
        if field_value in seen_values:
            # Combine tag information
            existing_track = seen_values[field_value]
            
            # Add the new tag_ranks to the existing one
            if 'tag_ranks' in track:
                if 'tag_ranks' in existing_track:
                    # Combine the tag_ranks strings
                    existing_track['tag_ranks'] = f"{existing_track['tag_ranks']}, {track['tag_ranks']}"
                else:
                    # If existing track doesn't have tag_ranks yet, just take the new one
                    existing_track['tag_ranks'] = track['tag_ranks']
            
        else:
            # Add to unique tracks and update seen values
            unique_tracks.append(track)
            seen_values[field_value] = track
    
    return unique_tracks


# After collecting all tracks
print("\n= = = = = = = = = = = = = = = = = =")
print("INITIAL TRACKS ANALYSIS")
print("= = = = = = = = = = = = = = = = = =")
tracks_with_url, unique_urls = analyze_tracks(all_tracks, "url")
tracks_with_mbid, unique_mbids = analyze_tracks(all_tracks, "mbid")

# Initialize tag_ranks before deduplication
all_tracks = initialize_tag_ranks(all_tracks)

# Remove duplicates based on URL
print("\n= = = = = = = = = = = = = = = = = =")
print("REMOVING DUPLICATES BY URL")
print("= = = = = = = = = = = = = = = = = =")
unique_tracks_by_url = remove_duplicates_by_field(all_tracks, "url")

print(f"Original tracks: {len(all_tracks)}")
print(f"Unique tracks by URL: {len(unique_tracks_by_url)}")
print(f"Removed {len(all_tracks) - len(unique_tracks_by_url)} duplicate tracks")

# Remove duplicates based on MBID (for tracks that have an MBID)
print("\n= = = = = = = = = = = = = = = = = =")
print("REMOVING DUPLICATES BY MBID")
print("= = = = = = = = = = = = = = = = = =")

# First, separate tracks with and without MBID
tracks_with_mbid_data = [track for track in unique_tracks_by_url if track.get('mbid')]
tracks_without_mbid = [track for track in unique_tracks_by_url if not track.get('mbid')]

# Remove duplicates from tracks with MBID
unique_tracks_by_mbid = remove_duplicates_by_field(tracks_with_mbid_data, "mbid")

# Combine the unique tracks with MBID and those without MBID
final_tracks = unique_tracks_by_mbid + tracks_without_mbid

print(f"Tracks after URL deduplication: {len(unique_tracks_by_url)}")
print(f"Final unique tracks (after MBID deduplication): {len(final_tracks)}")
print(f"Removed {len(unique_tracks_by_url) - len(final_tracks)} additional duplicate tracks by MBID")

# Remove duplicates based on name and artist name
print("\n= = = = = = = = = = = = = = = = = =")
print("REMOVING DUPLICATES BY NAME & ARTIST")
print("= = = = = = = = = = = = = = = = = =")

tracks_before_name_artist = len(final_tracks)
# Use the same function but pass a list of fields
final_tracks = remove_duplicates_by_field(final_tracks, ["name", "artist_name"])

print(f"Tracks before name+artist deduplication: {tracks_before_name_artist}")
print(f"Final unique tracks (after name+artist deduplication): {len(final_tracks)}")
print(f"Removed {tracks_before_name_artist - len(final_tracks)} additional duplicate tracks by name+artist")

# Final analysis
print("\n= = = = = = = = = = = = = = = = = =")
print("FINAL TRACKS ANALYSIS")
print("= = = = = = = = = = = = = = = = = =")
_, final_unique_urls = analyze_tracks(final_tracks, "url")
_, final_unique_mbids = analyze_tracks(final_tracks, "mbid")

# Example: Print a few tracks with their tag rankings
print("\nSample of tracks with their tag rankings:")
for i, track in enumerate(final_tracks[:5], 1):
    print(f"{i}. {track['name']} by {track['artist']['name']}")
    print(f"   Tag Rankings: {track.get('tag_ranks', {})}")
    print(track)
    print()


print("\n= = = = = = = = = = = = = = = = = =")
print("CREATING DATAFRAME FROM FILTERED TRACKS")
print("= = = = = = = = = = = = = = = = = =")

# Convert final_tracks to pandas DataFrame with selected columns
tracks_data = []
for track in final_tracks:
    # Extract the required fields
    track_name = track.get('name')
    artist_name = track.get('artist', {}).get('name')
    mbid = track.get('mbid')
    url = track.get('url')
    tag_ranks = track.get('tag_ranks')
    
    if mbid:  # Only include tracks with an MBID
        tracks_data.append({
            'name': track_name,
            'artist_name': artist_name,
            'mbid': mbid,
            'url': url,
            'tag_ranks': tag_ranks
        })

# Create DataFrame with the extracted data
last_fm_initial_data = pd.DataFrame(tracks_data)

print(f"Created DataFrame with {len(last_fm_initial_data)} tracks (all having MBIDs)")
print(f"DataFrame columns: {last_fm_initial_data.columns.tolist()}")

# Now retrieve detailed information for each track using track.getInfo API
print("\n= = = = = = = = = = = = = = = = = =")
print("RETRIEVING DETAILED TRACK INFO")
print("= = = = = = = = = = = = = = = = = =")

detailed_tracks = []
request_count = 0
success_count = 0
data_count = 0

# Iterate through each track in the DataFrame
for index, row in last_fm_initial_data.iterrows():
    mbid = row['mbid']
    
    # Set up API request parameters
    params = {
        "method": "track.getInfo",
        "api_key": API_KEY,
        "mbid": mbid,
        "format": "json"
    }
    
    # Track request count
    request_count += 1
    
    try:
        # Make API request with rate limiting
        response = requests.get(BASE_URL, params=params)
        
        # Track success count based on status code
        if response.status_code == 200:
            success_count += 1
            
            # Check if data was actually returned
            response_data = response.json()
            if 'track' in response_data:
                data_count += 1
                detailed_tracks.append(response_data)
            
            # Print progress update for every 10 tracks
            if request_count % 10 == 0:
                print(f"Processed {request_count} tracks, {success_count} successful, {data_count} with data")
        else:
            print(f"Request failed for MBID {mbid}: Status code {response.status_code}")
    except Exception as e:
        print(f"Error processing MBID {mbid}: {str(e)}")
    
    # Rate limiting
    time.sleep(0.26)

# Print statistics about the API requests
print("\n= = = = = = = = = = = = = = = = = =")
print("API REQUEST STATISTICS")
print("= = = = = = = = = = = = = = = = = =")
print(f"Total requests made: {request_count}")
print(f"Successful responses (200 status): {success_count}")
print(f"Responses with track data: {data_count}")
print(f"Success rate: {success_count/request_count*100:.2f}%")
print(f"Data success rate: {data_count/request_count*100:.2f}%")

# Display sample of the retrieved data
print("\n= = = = = = = = = = = = = = = = = =")
print("SAMPLE TRACK INFO (FIRST 3 TRACKS)")
print("= = = = = = = = = = = = = = = = = =")

# Display first 3 tracks (or fewer if less than 3 are available)
sample_count = min(3, len(detailed_tracks))
for i in range(sample_count):
    print(f"\nTrack {i+1}:")
    if 'track' in detailed_tracks[i]:
        track_info = detailed_tracks[i]['track']
        print(f"Name: {track_info.get('name')}")
        print(f"Artist: {track_info.get('artist', {}).get('name')}")
        print(f"Album: {track_info.get('album', {}).get('title')}")
        print(f"Listeners: {track_info.get('listeners')}")
        print(f"Playcount: {track_info.get('playcount')}")
    else:
        print("No track data available")

print(f"\nTotal detailed tracks retrieved: {len(detailed_tracks)}")












# If you want to maintain a dictionary for easier access by tag
# tracks_by_tag = {}
# for tag in top_tags[:max_tags]:
#     tracks_by_tag[tag] = [track for track in all_tracks if track.get('tag') == tag]


# for key, value in islice(all_tracks.items(), 1):