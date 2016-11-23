from flask import Flask, render_template
import sqlite3 as sql

from helpers import DbFunctions

app = Flask(__name__)


@app.route("/")
def latest_peak():

    # Get tweet db name
    db_request_obj = DbFunctions.DbRequests()
    tweets_db_name = db_request_obj.get_last_execution()

    # Get last tweet peak
    conn = sql.connect(tweets_db_name+'.db')
    conn.row_factory = sql.Row

    try:

        # Get peak info
        with conn:
            data = dict(list(conn.execute('select * from tweet_peaks ORDER by peak_datetime desc limit 1'))[0])
            data["Database name"] = tweets_db_name

        # Get plot info
        db_name = tweets_db_name.split("/")[2]
        img_name = db_name+"_"+data["peak_datetime"] + data["hashtag"]+".png"
        data["img_url"] = img_name

    except Exception as e:

        data = {"message: ": "No peak found",
                "exception": e,
                "Database name": tweets_db_name}

    return render_template('twitter_app.html', data=data)


def set_flask():
    """
    Interface to start flask server from main
    """
    app.run(host='0.0.0.0',
            port=5010,
            debug=False)


if __name__ == "__main__":

    set_flask()
