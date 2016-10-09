import sqlite3 as sql
import pandas as pd
from datetime import datetime, timedelta
import peak_detection as peak
import numpy as np
from datetime import datetime


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

    def time_frame_evaluation(self, time_frame, time_shift_minutes):
        """
        Set data series into a given time_frame
        It will also clean the table to maintain the data in the given period of time
        """

        time = datetime.utcnow() - timedelta(hours=0, minutes=time_shift_minutes, seconds=0)

        with self.conn:
            # @TODO: Not cool delete data from here! Change it!
            # Delete old data from the tweet table
            drop_query = """DELETE FROM tweets WHERE created_at <= ?;"""
            self.conn.execute(drop_query, (time.strftime('%a %b %d %H:%M:%S %z %Y'), ))

            # Get valid twitter data
            query = "select * from tweets"
            proc_data = self.conn.execute(query)
            cols = ["id", "tweet_id", "insert_date", "created_at", "hashtag"]
            tweets = pd.DataFrame.from_records(data=proc_data.fetchall(), columns=cols)

        # Handle with twitter date format
        tweets['agg_created_at'] = tweets['created_at'].map(lambda x: set_datetime_format(x))
        tweets = tweets.set_index(['agg_created_at'])

        # Get time window in seconds
        time_window = (max(list(tweets.index))-min(list(tweets.index)))/np.timedelta64(1, 's')

        for hashtag, hashtag_tweets in tweets.groupby('hashtag'):
            qt_tweets = hashtag_tweets.tweet_id.resample(time_frame).count()
            peak.peak_detection(hashtag, qt_tweets, time_window, time_frame)


if __name__ == '__main__':
    table = TweetAggregation()
    table.time_frame_evaluation(time_frame='1min', time_shift_minutes=70)