from flask import Flask, render_template_string, request
import psycopg2
import json
import datetime

app = Flask(__name__)

# Database connection config
DB_CONFIG = {
    "host": "postgres",
    "dbname": "group6",
    "user": "group6",
    "password": "group6",
    "port": "5432"
}

def safe_serialize(row_dict):
    return {
        key: (value.isoformat() if isinstance(value, (datetime.date, datetime.datetime)) else value)
        for key, value in row_dict.items()
    }

@app.route('/cross-platform-consensus', methods=["GET", "POST"])
def show_cross_platform_consensus():
    filter_artist = None
    filter_values = []

    base_conditions = """
        sp.popularity > 70 AND 
        lf.playcount > 100000 AND 
        bl.total_wks_on_chart > 5
    """

    where_clause = f"WHERE {base_conditions}"

    if request.method == "POST":
        filter_artist = request.form.get("artist_name")
        if filter_artist:
            where_clause += " AND LOWER(sp.top_artist) LIKE %s"
            filter_values.append(f"%{filter_artist.lower()}%")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query = f"""
        SELECT DISTINCT
            sp.top_artist AS spotify_artist,
            sp.song_name AS track_name,
            sp.popularity AS spotify_popularity,
            lf.playcount AS lastfm_playcount,
            lf.listeners AS lastfm_listeners,
            lf.tag_ranks AS lastfm_tags,
            bl.peak_chart_pos AS billboard_peak_position,
            bl.total_wks_on_chart AS billboard_weeks_on_chart
        FROM public.spotify_tracks sp
        JOIN public.lastfm_tracks lf
            ON sp.song_name = lf.song_name AND sp.top_artist = lf.artist
        JOIN public.billboard_chart_data bl
            ON sp.song_name = bl.song_name AND sp.top_artist = ANY(string_to_array(bl.artists, ','))
        {where_clause}
        ORDER BY bl.peak_chart_pos ASC, sp.popularity DESC
        LIMIT 50
    """

    cur.execute(query, filter_values)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    data = [safe_serialize(dict(zip(column_names, row))) for row in rows]
    print(json.dumps(data, indent=4))

    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Cross-Platform Consensus Tracks</title>
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            form { margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <h2>Cross-Platform Consensus Tracks</h2>
        <form method="POST">
            <label for="artist_name">Filter by Artist Name:</label>
            <input type="text" id="artist_name" name="artist_name" placeholder="Enter artist name" value="{{ filter_artist or '' }}">
            <button type="submit">Filter</button>
        </form>
        <table>
            <tr>
                {% for col in column_names %}
                <th>{{ col }}</th>
                {% endfor %}
            </tr>
            {% for row in rows %}
            <tr>
                {% for cell in row %}
                <td>{{ cell }}</td>
                {% endfor %}
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """

    return render_template_string(html_template, rows=rows, filter_artist=filter_artist, column_names=column_names)

@app.route('/merged-tracks', methods=["GET", "POST"])
def show_merged_tracks():
    filter_artist = None
    filter_clause = ""
    filter_values = []

    if request.method == "POST":
        filter_artist = request.form.get("artist_name")
        if filter_artist:
            filter_clause = "WHERE LOWER(sp.top_artist) LIKE %s"
            filter_values.append(f"%{filter_artist.lower()}%")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query = f"""
        SELECT DISTINCT 
            sp.top_artist,
            sp.song_name,
            lf.artist,
            lf.song_name,
            mbid
        FROM public.spotify_tracks sp
        JOIN public.lastfm_tracks lf 
            ON sp.song_name = lf.song_name 
            AND sp.top_artist = lf.artist
        JOIN public.billboard_chart_data bl 
            ON sp.song_name = bl.song_name 
            AND sp.top_artist = ANY(string_to_array(bl.artists, ','))
        {filter_clause}
    """

    cur.execute(query, filter_values)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()


    data = [dict(zip(column_names, row)) for row in rows]
    print(json.dumps(data, indent=4))  

    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Merged Tracks</title>
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            form { margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <h2>Merged Track Records</h2>
        <form method="POST">
            <label for="artist_name">Filter by Artist Name:</label>
            <input type="text" id="artist_name" name="artist_name" placeholder="Enter artist name" value="{{ filter_artist or '' }}">
            <button type="submit">Filter</button>
        </form>
        <table>
            <tr>
                <th>Spotify Artist</th>
                <th>Spotify Song</th>
                <th>Last.fm Artist</th>
                <th>Last.fm Song</th>
                <th>MBID</th>
            </tr>
            {% for row in rows %}
            <tr>
                {% for cell in row %}
                <td>{{ cell }}</td>
                {% endfor %}
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """

    return render_template_string(html_template, rows=rows, filter_artist=filter_artist)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
