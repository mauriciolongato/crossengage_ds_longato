import sqlite3 as sql

conn = sql.connect('./twitter_streaming_data.db')
# Create table that will contain twitter hashtag info
conn.execute("""CREATE TABLE IF NOT EXISTS tweets(
                id INTEGER PRIMARY KEY
                , tweet_id INTEGER
                , insert_date TEXT
                , created_at TEXT
                , hashtag TEXT
                )
            """)

conn.close()