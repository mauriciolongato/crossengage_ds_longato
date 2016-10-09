import unittest
from unittest import TestCase
from pandas import Series
from scipy.stats import norm
import scipy.stats
import logging
from datetime import datetime
import sys
import sqlite3 as sql


# Set log config

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
# create file handler and ser level to debug
fh = logging.FileHandler('log/peak_detection.txt')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)
logger.addHandler(ch)


def probability_calc(mu, std, frequency):
    """
    Function that evaluates probability of a given frequecy considering a
    normal distribution

    :param mu: Average of qt_tweet/time frame
    :param std: Standard Seviation
    :param frequency: Pandas DataFrame
    :return: DataFrame containing the probability of each frequency
    """
    return 1-scipy.stats.norm(mu, std).cdf(frequency)


def peak_detection(hashtag, count_series, time_window, time_frame, sensibility = 0.98, freq_lim = 0.08):
    """
    function to detect peaks of a certain hashtag
    peak will be difined as:
        - Probability of occurence of a gave frequency <= sensibility
        - Will be evaluate peaks in data series that has a tweet frequency grater than "freq_lim"
          (tweet frequency = tweets per second)

    :type count_series: pd.Series
    """
    # Calculate the occurence's probability of a given frequency - Using normal distribution
    (mu, std) = norm.fit(count_series)
    frequencies = count_series.to_frame()
    frequencies['probability'] = frequencies['tweet_id'].apply(lambda x: probability_calc(mu, std, x))

    # Evaluates the peaks considering tweets/s of a given hashtag and the probability of a frequency
    tweet_frequency = float(count_series.sum())/time_window
    if tweet_frequency >= freq_lim:

        logger.debug('Hashtag candidate to have a peak: %s - %s tweets/s', hashtag, tweet_frequency)
        peaks = frequencies[frequencies['probability'] <= (1-sensibility)]

        # Built a dict containing peak's information
        result = {}
        result['hashtag'] = hashtag
        result['criteria'] = {'sensibility': sensibility, 'freq_lim': freq_lim}
        result['stats'] = {'mean' : mu, 'std' : std}
        result['peaks'] = peaks['tweet_id'].to_dict()
        result['peaks_info'] = {'quantity': peaks['tweet_id'].to_dict(), 'probability': peaks['tweet_id'].to_dict()}

        # Insert into the database
        peak_list_info = []
        for peak_time, qt_tweets in result['peaks'].items():

            peak = (str(peak_time)+hashtag, str(peak_time), time_frame, hashtag, mu, std, sensibility, freq_lim
                    , int(qt_tweets), peaks['probability'][peak_time])

            peak_list_info.append(peak)
            logger.debug('Found peak %s', peak)

        query = """insert into
                    tweet_peaks(id, peak_datetime, time_frame, hashtag, mean, std, sensibility
                                , freq_limit, qt_tweets, probability)
                    values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        with sql.connect('./twitter_streaming_data.db') as conn:
            try:
                conn.executemany(query, peak_list_info)
            except Exception as e:
                logger.exception(e)

    else:
        logger.debug(" No peak %s - tweet frequency: %s tweets/s", hashtag, tweet_frequency)

class Peak_detection_test_case(TestCase):

    def initialize_test(self):
        tweets = Series([1, 23233, 2, 3])

        return peak_detection(tweets)

    #metodo tem q comecar com test_
    def test_check_for_null(self):
        peaks = self.initialize_test()

        self.assertIsNotNone(peaks)

    def test_len(self):
        peaks = self.initialize_test()

        self.assertAlmostEqual(len(peaks), 2, places=3)

if __name__ == '__main__':
    unittest.main()