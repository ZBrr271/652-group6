from flask import Flask, render_template_string
import psycopg2

app = Flask(__name__)

# Database connection config
DB_CONFIG = {
    "host": "localhost",
    "dbname": "your_db",
    "user": "your_user",
    "password": "your_password",
    "port": "5432"
}

@app.route('/tracks', methods=['GET'])
def show_tracks():
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Fetch top 20 tracks
    cur.execute("""
        SELECT track_name, artist_name, tag, rank
        FROM lastfm_top_tag_tracks
        ORDER BY rank ASC
        LIMIT 20
    """)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    # HTML Template
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Top Last.fm Tracks</title>
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            table { border-collapse: collapse; width: 80%; margin: auto; }
            th, td { padding: 10px; border: 1px solid #ccc; text-align: left; }
            th { background-color: #f2f2f2; }
            h1 { text-align: center; }
        </style>
    </head>
    <body>
        <h1>Top 20 Last.fm Tracks</h1>
        <table>
            <tr><th>Track Name</th><th>Artist</th><th>Tag</th><th>Rank</th></tr>
            {% for row in rows %}
                <tr>
                    <td>{{ row[0] }}</td>
                    <td>{{ row[1] }}</td>
                    <td>{{ row[2] }}</td>
                    <td>{{ row[3] }}</td>
                </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """

    return render_template_string(html_template, rows=rows)

if __name__ == '__main__':
    app.run(port=5001, debug=True)
