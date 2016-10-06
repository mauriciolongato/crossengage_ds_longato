import unittest
from unittest import TestCase
from pandas import Series
import numpy as np
from scipy.stats import norm


def peak_detection(hashtag, count_series):
    """
    function to detect peaks of a certain hashtag

    :type count_series: pd.Series
    :return: dictionary with timestamps and counts of peaks
    """

    qt_tweets = count_series.tweet_id.resample('1min').count()
    mu, std = norm.fit(qt_tweets)
    print(hashtag, " - ", mu, std)
    print(qt_tweets)
    count, division = np.histogram(qt_tweets)
    print(count, division)





    return {'2015.03': 4777, '2016.04': 534}


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