import datetime
import time
import dateutil.parser
import logging

import helpers.peak_detection as peak
from helpers import DbFunctions
from helpers.data_type import time_frame_seconds


# Set log config

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler and ser level to debug
fh = logging.FileHandler('log/twitter_analyser.txt')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)


class TweetAggregation:
    """
    Class that manages the analysis tweets
     - Aggregates over time_frame
     - Calls peak_detection
    """

    #TODO: Change db_obj name
    def __init__(self, db_obj, time_frame):
        self.db_obj = db_obj
        self.time_frame = time_frame

    def time_frame_evaluation(self, sensibility, minimum_tweet_per_sec, dataset_size):
        """
        Set data series into a given time_frame
        It will also clean the table to maintain the data in the given period of time
        """
        # Get time interval that will be analysed
        time_f = self.db_obj.get_time_last_tweet_tweets()
        time_i = dateutil.parser.parse(time_f) - datetime.timedelta(seconds=dataset_size)
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
                   analysis_sample_size,
                   data_base):

    # Analysis must be started only after the minimum sample_size are recorded
    time_frame_s = time_frame_seconds(time_frame[0])
    dataset_size = time_frame_s * analysis_sample_size[0]
    time.sleep(dataset_size)

    table = TweetAggregation(data_base, time_frame[0])
    flag = True

    # Analyses existence of a peak every time that a frame is complete
    while flag:

        try:
            table.time_frame_evaluation(sensibility=peak_detection_sensibility[0],
                                        minimum_tweet_per_sec=minimum_tweet_per_sec[0],
                                        dataset_size=dataset_size)

            time.sleep(time_frame_s)
            flag = True

        except Exception as e:
            print("except", e)
            time.sleep(time_frame_seconds(time_frame))


if __name__ == '__main__':

    db_obj = DbFunctions.DbFunctions("teste_set")
    start_analyzer([0.98], [0.1], ["30s"], [1], db_obj)