import sqlite3 as sql
import pandas as pd
from datetime import datetime
import peak_detection as peak


def set_datetime_format(tweet_created_at):
    format = '%a %b %d %H:%M:%S %z %Y'
    return datetime.strptime(tweet_created_at, format)


class TweetAggregation:
    """
    Class that handle with data aspects from tweets
     - Connects with database
     - Aggregates over time_frame
     - Calls peak_detection
    """

    def __init__(self):
        self.conn = sql.connect('./twitter_streaming_data.db')

    def time_frame_evaluation(self, time_frame="1Min"):
        """
        function that connects to the tweet table from twitter_streaming_data.db
         and aggregates over time_frame

        """

        # Connect to the tweets database
        query = "select * from tweets"
        with self.conn:
            proc_data = self.conn.execute(query)
            cols = ["id", "tweet_id", "insert_date", "created_at", "hashtag"]
            tweets = pd.DataFrame.from_records(data=proc_data.fetchall(), columns=cols)

        # Handle with twitter date format
        tweets['agg_created_at'] = tweets['created_at'].map(lambda x: set_datetime_format(x))
        tweets = tweets.set_index(['agg_created_at'])

        for hashtag, hashtag_tweets in tweets.groupby('hashtag'):
            # We will find spikes in the time series
            # Group datetime in the given timeframe - Change per minute
            qt_tweets = hashtag_tweets.tweet_id.resample(time_frame).count()
            peak.peak_detection(hashtag, qt_tweets)


if __name__ == '__main__':
    table = TweetAggregation()
    table.time_frame_evaluation(time_frame='1min')