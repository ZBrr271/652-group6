import psycopg2
import pandas as pd
import json

# Connection details - update these with your actual connection info
conn_params = {
    "host": "localhost",
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": "5432"
}

# Connect to the database
try:
    conn = psycopg2.connect(**conn_params)
    print("Connected to the database successfully")
    
    # Create a cursor
    cursor = conn.cursor()
    
    # Execute a query to get a few rows from lastfm_tracks
    cursor.execute("SELECT artist, song_name, album_name, tag_ranks FROM lastfm_tracks LIMIT 5")
    rows = cursor.fetchall()
    
    print("\nSample data from lastfm_tracks:")
    for i, row in enumerate(rows):
        print(f"\nRow {i+1}:")
        print(f"Artist: {row[0]} (type: {type(row[0])})")
        print(f"Song name: {row[1]} (type: {type(row[1])})")
        print(f"Album name: {row[2]} (type: {type(row[2])})")
        print(f"Tag ranks: {row[3]} (type: {type(row[3])})")
        
        # If tag_ranks is a string that looks like JSON, try to parse it
        if isinstance(row[3], str) and row[3].startswith('{'):
            try:
                parsed = json.loads(row[3])
                print(f"Tag ranks (parsed): {parsed}")
            except json.JSONDecodeError:
                print("Could not parse tag_ranks as JSON")
    
    # Now let's check the table definition
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'lastfm_tracks'
    """)
    columns = cursor.fetchall()
    
    print("\nTable structure:")
    for col in columns:
        print(f"Column: {col[0]}, Type: {col[1]}")
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}") 