import requests
import base64
from urllib.parse import urlencode
import time
# from tabulate import tabulate
import pandas as pd

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

# Random comment
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


# List of artist-song pairs
track_list = [
    {"artist": "Mariah Carey", "track": "All I Want For Christmas Is You"},
    {"artist": "Brenda Lee", "track": "Rockin' Around The Christmas Tree"},
    {"artist": "Bobby Helms", "track": "Jingle Bell Rock"},
    {"artist": "Burl Ives", "track": "A Holly Jolly Christmas"},
    {"artist": "Adele", "track": "Easy On Me"},
    {"artist": "Andy Williams", "track": "It's The Most Wonderful Time Of The Year"},
    {"artist": "Wham!", "track": "Last Christmas"},
    {"artist": "Jose Feliciano", "track": "Feliz Navidad"},
    {"artist": "The Kid LAROI & Justin Bieber", "track": "Stay"},
    {"artist": "The Ronettes", "track": "Sleigh Ride"},
]



# Set the base URLs and search parameters
search_url = "https://api.spotify.com/v1/search"



headers = {"Authorization": f"Bearer {token}"}

# Store results
track_data_list = []

for track in track_list:
    artist = track["artist"]
    song = track["track"]

    params = {
        "q": f"track:{song} artist:{artist}",
        "type": "track",
        "limit": 1
    }

    response = requests.get(f"{search_url}?{urlencode(params)}", headers=headers)

    if response.status_code == 200:

        search_results = response.json()
        items = search_results.get("tracks", {}).get("items", [])

        if items:
            
            track_id = items[0]["id"]

            track_info = items[0]

            track_details = {
                "artist": artist,
                "song": song,
                "id": track_info.get("id"),
                "duration_ms": track_info.get("duration_ms"),
                "popularity": track_info.get("popularity"),
                "album_name": track_info.get("album", {}).get("name"),
                "release_date": track_info.get("album", {}).get("release_date"),
                "explicit": track_info.get("explicit"),
                "isrc": track_info.get("external_ids", {}).get("isrc")
            }

            track_data_list.append(track_details)
            print(f"Processed: {song} by {artist}")
    
        else:
            print(f"No results found for {song} by {artist}")
    else:
        print(f"Error searching for {song} by {artist}. Status: {response.status_code}")

    print("\n\n\n\n\n")    
    time.sleep(1)

print("\nFinal Track Data List: ")
for entry in track_data_list:
    print(entry)

#table_headers = track_data_list[0].keys() if track_data_list else []
#table = [list(track.values()) for track in track_data_list]
#print(tabulate(table, headers=table_headers, tablefmt="grid"))

df = pd.DataFrame(track_data_list)
print(df.to_string(index=False))


"""
# Encode the parameters into a query string
query_string = urlencode(params)

# Combine base URL with the encoded query string
url = f"{base_url}?{query_string}"

# Set the request headers
headers = {
    "Authorization": f"Bearer {token}"
}

# Make the GET request
response = requests.get(url, headers=headers)

# Handle the response
if response.status_code == 200:
    #print(response.json())  # Successful response
    print("Success")
    #first_result = response.json().get("tracks", {}).get("items", [])[0]
    first_result = response.json().get("tracks", {}).get("items", [])[0]
    print(first_result)
    spotifyid = first_result.get("id")
    print("id: ", spotifyid)
else:
    print("Error:", response.status_code, response.text)  # Handle errors
"""
