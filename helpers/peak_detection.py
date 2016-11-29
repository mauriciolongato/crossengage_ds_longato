import csv
import logging
import pandas as pd
import scipy.stats

from helpers.plot import create_plot

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler and ser level to debug
fh = logging.FileHandler('log/peak_detection.txt')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)


def probability_calc(mu, std, frequency):
    """
    Function that evaluates probability of a given frequency considering a
    normal distribution

    :param mu: Average of qt_tweet/time frame
    :param std: Standard Deviation
    :param frequency: frequency of tweets
    :return: probability of the frequency
    """
    return 1 - scipy.stats.norm(mu, std).cdf(frequency)


def peak_detection(hashtag, count_series, time_window, time_frame,
                   sensibility, minimum_tweet_per_sec, db_obj):
    """
    function to detect peaks of a certain hashtag
    peak will be defined as:
        - Probability of occurrence of a given frequency <= 1- sensibility

    Evaluate peaks in a data series that has tweet frequency greater than minimum_tweet_per_sec
          (tweet frequency = tweets per second)

    :type count_series: pd.Series
    """

    # Evaluates the peaks considering tweets/s of a given hashtag and the probability of a frequency
    tweet_frequency = float(count_series.sum()) / time_window

    if tweet_frequency >= minimum_tweet_per_sec:

        # Calculate the occurrence's probability of a given frequency - Using normal distribution
        (mu, std) = scipy.stats.norm.fit(count_series)
        frequencies = count_series.to_frame(name='count')

        frequencies['probability'] = frequencies['count'].apply(lambda x: probability_calc(mu, std, x))

        logger.debug('Hashtag candidate to have a peak: %s - %s tweets/s', hashtag, tweet_frequency)
        peaks = frequencies[frequencies['probability'] <= (1 - sensibility)]

        # Insert into the database
        peak_list_info = []
        for peak_time, qt_tweets in peaks['count'].to_dict().items():
            # key are composed by time from peak and the correspondent hashtag
            peak = (str(peak_time) + hashtag, str(peak_time),
                    time_frame, hashtag, mu, std, sensibility,
                    minimum_tweet_per_sec, int(qt_tweets),
                    peaks['probability'][peak_time])

            peak_list_info.append(peak)
            logger.debug('Found peak %s', peak)

            # Save data from the peak - csv and a plot.jpeg
            db_name = db_obj.name.split("/")[2]
            frequencies.to_csv("./peak_data_history/" + db_name + "_" + str(peak_time) + hashtag + ".csv",
                               sep=';', quoting=csv.QUOTE_ALL, quotechar='"')

            plot_name = "./static/" + db_name + "_" + str(peak_time) + hashtag
            create_plot(frequencies, plot_name, str(peak_time), hashtag)

        db_obj.insert_into_tweet_peaks(peak_list_info)

    else:
        logger.debug(" No peak %s - tweet frequency: %s tweets/s", hashtag, tweet_frequency)
