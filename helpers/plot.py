from matplotlib import pyplot as plt
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler and ser level to debug
fh = logging.FileHandler('log/plot.txt')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)

def create_plot(frequencies, file_name, peak_time, hashtag):
    """
    :param frequencies: pandas DataFrame
    :param file_name: composition of: ./plots + database_name + datetime + hastag
    :param peak_time: time that the peak has been found (str)
    :param hashtag: hashtag (str)
    """
    try:

        fig = plt.figure(figsize=(11, 8))  # Create matplotlib figure  # Create matplotlib figure
        # Set Axe_1
        ax = fig.add_subplot(111)  # Create matplotlib axes
        # Set Axe_2
        ax2 = ax.twinx()  # Create another axes that shares the same x-axis as ax.

        plt.title(peak_time + " - " + hashtag, fontsize=20)
        ax.set_xlabel("aggregated time frame", fontsize=16)
        ax.set_ylabel("#tweets/time_frame", fontsize=16)
        ax2.set_ylabel("1 - Acc. P(#tweets)", fontsize=16)

        width = 0.4

        qt_tweets = frequencies["count"]
        probability = frequencies["probability"]

        qt_tweets.plot(kind='bar', color='navy', ax=ax, width=width)
        probability.plot(marker='o', color='orange', linewidth=2.0, ax=ax2)

        fig.tight_layout()
        plt.savefig(file_name + '.png')

        fig.clf()
        plt.close()

    except Exception:

        logger.exception("message")
        logger.info("Not able to create plot from the file {}".format(file_name))