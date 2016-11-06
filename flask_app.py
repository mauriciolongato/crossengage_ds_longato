from flask import Flask, render_template
import sqlite3 as sql
app = Flask(__name__)


@app.route("/")
def latest_peak():
    #@TODO: Pass db object as parameter to the function
    #conn = sql.connect('twitter_streaming_data.db')
    conn = sql.connect('./example_db/twitter_streaming_data_peak_tests.db')
    conn.row_factory = sql.Row

    with conn:
        data = dict(list(conn.execute('select * from tweet_peaks ORDER by peak_datetime desc limit 1'))[0])
    return render_template('twitter_app.html', data=data)

if __name__ == "__main__":
    app.run(host='0.0.0.0',
            port=5001,
            debug=True)
