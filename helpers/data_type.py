import datetime
import re


def set_datetime_format(tweet_created_at):
    format = '%a %b %d %H:%M:%S %z %Y'
    return datetime.datetime.strptime(tweet_created_at, format)

def time_frame_seconds(time_frame):
    """
    Receives time_frame in the format "[num][unit]" and returns the time in seconds

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