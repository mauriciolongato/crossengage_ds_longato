from datetime import datetime
import datetime
import dateutil.parser
import time
import re

import set_db
import peak_detection as peak


def set_datetime_format(tweet_created_at):
    format = '%a %b %d %H:%M:%S %z %Y'
    return datetime.datetime.strptime(tweet_created_at, format)

def time_frame_seconds(time_frame):
    """
    Function receive time frame num.unit and return the time in seconds

    :param time_frame: time frame
    :return: time frame in seconds
    """
    frames = {'s': 1, 'min': 60, 'hour': 3600}

    match = re.match(r"([0-9]+)([a-z]+)", time_frame, re.I)
    items = match.groups()

    return int(items[0]) * frames[items[1]]

def twitter_analyser_periodicity(time_frame, periodicity):
    """
    Evaluate the period in seconds between two executions of twitter_analyser

    :param time_frame: Parameter given by the user to set how to frame time data from twitter
    :param periodicity: How many time frames are necessary to evaluate peak
    :return: period between the executions of twitter_analyser in seconds
    """
    time_frame_s = time_frame_seconds(time_frame)

    period = time_frame_s * periodicity

    return period

class TweetAggregation:
    """
    Class that manages the analysis tweets
     - Aggregates over time_frame
     - Calls peak_detection
    """

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
                   analysis_dataset_size,
                   data_base):

    # Analysis must be started only after the minimum dataset_size are settled
    dataset_size_sec = twitter_analyser_periodicity(time_frame[0], analysis_dataset_size)
    time.sleep(dataset_size_sec)

    table = TweetAggregation(data_base, time_frame[0])
    flag = True

    # Fuction analyse existense of a peak every time that a frame is complete
    while flag == True:

        try:
            table.time_frame_evaluation(sensibility=peak_detection_sensibility[0],
                                        minimum_tweet_per_sec=minimum_tweet_per_sec[0],
                                        dataset_size=dataset_size_sec)

            time.sleep(time_frame_seconds(time_frame[0]))
            flag = True

        except Exception as e:
            print("except", e)
            time.sleep(time_frame_seconds(time_frame))


if __name__ == '__main__':
    db_obj = set_db.instance_db("teste_set")
    #db_obj = set_db.instance_db('8b98dfd4-9e0a-11e6-a95f-801934389802')
    start_analyzer([0.98], [0.1], ["30s"], 1, db_obj)