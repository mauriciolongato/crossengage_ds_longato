import scipy.stats
import set_db
import logging
import sys


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
    Function that evaluates probability of a given frequency considering a
    normal distribution

    :param mu: Average of qt_tweet/time frame
    :param std: Standard Deviation
    :param frequency: frequency of tweets
    :return: probability of the frequency
    """
    return 1-scipy.stats.norm(mu, std).cdf(frequency)


def peak_detection(hashtag,
                   count_series,
                   time_window,
                   time_frame,
                   sensibility,
                   minimum_tweet_per_sec,
                   db_obj):
    """
    function to detect peaks of a certain hashtag
    peak will be defined as:
        - Probability of occurrence of a given frequency <= sensibility
        - Will evaluate peaks in a data series that has tweet frequency greater than freq_lim
          (tweet frequency = tweets per second)

    :type count_series: pd.Series
    """

    # Evaluates the peaks considering tweets/s of a given hashtag and the probability of a frequency
    tweet_frequency = float(count_series.sum())/time_window
    if tweet_frequency >= minimum_tweet_per_sec:

        # Calculate the occurrence's probability of a given frequency - Using normal distribution
        (mu, std) = scipy.stats.norm.fit(count_series)
        frequencies = count_series.to_frame()
        frequencies['probability'] = frequencies['tweet_id'].apply(lambda x: probability_calc(mu, std, x))

        logger.debug('Hashtag candidate to have a peak: %s - %s tweets/s', hashtag, tweet_frequency)
        peaks = frequencies[frequencies['probability'] <= (1-sensibility)]

        # Built a dict containing peak's information
        result = {}
        result['hashtag'] = hashtag
        result['criteria'] = {'sensibility': sensibility, 'freq_lim': minimum_tweet_per_sec}
        result['stats'] = {'mean' : mu, 'std' : std}
        result['peaks'] = peaks['tweet_id'].to_dict()
        result['peaks_info'] = {'quantity': peaks['tweet_id'].to_dict(), 'probability': peaks['tweet_id'].to_dict()}

        # Insert into the database
        peak_list_info = []
        for peak_time, qt_tweets in result['peaks'].items():

            peak = (str(peak_time)+hashtag,
                    str(peak_time),
                    time_frame,
                    hashtag,
                    mu,
                    std,
                    sensibility,
                    minimum_tweet_per_sec,
                    int(qt_tweets),
                    peaks['probability'][peak_time])

            peak_list_info.append(peak)
            logger.debug('Found peak %s', peak)

        db_obj.insert_into_tweet_peaks(peak_list_info)

    else:
        logger.debug(" No peak %s - tweet frequency: %s tweets/s", hashtag, tweet_frequency)