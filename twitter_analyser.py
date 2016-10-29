from datetime import datetime
import datetime
import dateutil.parser
import time

import set_db
import peak_detection as peak


def set_datetime_format(tweet_created_at):
    format = '%a %b %d %H:%M:%S %z %Y'
    return datetime.datetime.strptime(tweet_created_at, format)


class TweetAggregation:
    """
    Class that manages the analysis tweets
     - Aggregates over time_frame
     - Calls peak_detection
    """

    def __init__(self, db_obj, time_frame):
        self.db_obj = db_obj
        self.time_frame = time_frame

    def time_frame_evaluation(self, sensibility, minimum_tweet_per_sec):
        """
        Set data series into a given time_frame
        It will also clean the table to maintain the data in the given period of time
        """
        # Get time interval that will be analysed
        time_f = self.db_obj.get_time_last_tweet_tweets()
        time_i = dateutil.parser.parse(time_f) - datetime.timedelta(minutes=10)
        time_i = time_i.isoformat()

        # Get tweets from db
        tweets = self.db_obj.get_tweets_data_interval(time_i, time_f)
        time_window = dateutil.parser.parse(max(tweets["created_at"]))-dateutil.parser.parse(min(tweets["created_at"]))
        time_window = time_window.total_seconds()

        # Aggregate data
        tweets['agg_created_at'] = tweets['created_at'].apply(dateutil.parser.parse)
        tweets = tweets.set_index(['agg_created_at'])

        for hashtag, hashtag_tweets in tweets.groupby('hashtag'):

            qt_tweets = hashtag_tweets.tweet_id.resample(self.time_frame).count()
            peak.peak_detection(hashtag, qt_tweets, time_window,
                                self.time_frame, sensibility,
                                minimum_tweet_per_sec, self.db_obj)


def start_analyzer(peak_detection_sensibility,
                   minimum_tweet_per_sec,
                   time_frame,
                   data_base):

    table = TweetAggregation(data_base, time_frame[0])
    flag = True
    while flag == True:
        #TODO: Create function - from time_frame to seconds
        #time.sleep(30*5)
        table.time_frame_evaluation(sensibility=peak_detection_sensibility[0],
                                    minimum_tweet_per_sec=minimum_tweet_per_sec)
        time.sleep(30 * 5)

    pass

if __name__ == '__main__':
    #db_obj = set_db.instance_db("teste_set")
    db_obj = set_db.instance_db('b97b4f1e-9df5-11e6-a95f-801934389802')
    start_analyzer([0.98], 0.1, ["30s"], db_obj)