import pandas as pd
from datetime import datetime
import set_db
import peak_detection as peak
import numpy as np
from datetime import datetime
import dateutil.parser


#@TODO: put this function in the set_db.py
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

    def __init__(self, db_obj):
        self.conn = db_obj.conn

    def time_frame_evaluation(self
                              , time_frame
                              , sensibility
                              , minimum_tweet_per_sec):
        """
        Set data series into a given time_frame
        It will also clean the table to maintain the data in the given period of time
        """

        # time = datetime.utcnow() - timedelta(hours=0, minutes=time_shift_minutes, seconds=0)

        '''
        with self.conn:
            # Get Twitter data
            # @TODO: Change query in order to limit the interval of analisys
            query = "select * from tweets;"
            proc_data = self.conn.execute(query)
            cols = ["id", "tweet_id", "insert_date", "created_at", "hashtag"]
            tweets = pd.DataFrame.from_records(data=proc_data.fetchall(), columns=cols)
        '''

        #@TODO: Check if this function is working
        tweets = db_obj.get_tweets_data()
        # Handle with twitter date format
        tweets['agg_created_at'] = tweets['created_at'].apply(dateutil.parser.parse)
        tweets = tweets.set_index(['agg_created_at'])


        #@TODO: Check if timedelta64 stil make sense
        # Get time window in seconds
        time_window = (max(tweets.index)-min(tweets.index))/np.timedelta64(1, 's')

        #@TODO: Design how we will choose data set to be analised for peak_detection
        # For a given hashtag:
        #   a. Define time shift (Ex: periodo 1min. I want want at least 10 intervals to build
        #      the distribution. So we will have 1 min * 10 = 10 minutes of data
        #   b. Get datetime from tweets table that are in the specified interval
        #   c. To garantee we will have complete intervals with data, we will dismiss the last
        #      minute and the first minute
        #@TODO: Create a function on set_db that receive time interval as parameter and return pd.DAtaFrame

        time_i = datetime.utcnow().isoformat()
        time_f =  time_i - datetime.timedelta(minutes=10)

        time_f = db_obj.get_time_last_tweet_tweets()
        time_i = time_f
        for hashtag, hashtag_tweets in tweets.groupby('hashtag'):
            qt_tweets = hashtag_tweets.tweet_id.resample(time_frame).count()
            peak_list_info = peak.peak_detection(hashtag,
                                                 qt_tweets,
                                                 time_window,
                                                 time_frame,
                                                 sensibility,
                                                 minimum_tweet_per_sec,
                                                 db_obj)



def start_analyzer(peak_detection_sensibility,
                   minimum_tweet_per_sec,
                   time_frame,
                   data_base):

    table = TweetAggregation(data_base)
    table.time_frame_evaluation(time_frame=time_frame[0],
                                sensibility=peak_detection_sensibility[0],
                                minimum_tweet_per_sec=minimum_tweet_per_sec)

    pass

if __name__ == '__main__':
    #table = TweetAggregation()
    #table.time_frame_evaluation(time_frame='1min')
    db_obj = set_db.instance_db("teste_set")
    start_analyzer([0.98], 0.001, ["1min"], db_obj)