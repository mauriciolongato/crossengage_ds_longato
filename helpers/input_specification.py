import argparse
import re


def location_coord(s):
    """
    Handle coordinates input

    :param s: string containing coordinates
    :return: tuple of coordinates
    """
    try:
        locations = tuple(map(float, s.split(',')))
        if len(locations) > 0:
            if len(locations) % 4 == 0:
                return locations
            else:
                raise argparse.ArgumentTypeError("Number of coordinates must be a multiple of 4")
        else:
            raise argparse.ArgumentTypeError("Coordinates must be sw_lon, sw_lat, ne_lon, ne_lat")
    except:
        raise argparse.ArgumentTypeError("Coordinates must be sw_lon, sw_lat, ne_lon, ne_lat")

def track_list(s):
    """
    Handle topic list

    :param s: string containing words separated by comma
    :return: list of strings
    """

    try:
        s_list = s.split(',')
        return s_list
    except:
        raise argparse.ArgumentTypeError("Tracks must be a string of tags separated by comma")

def set_sensibility(s):
    """
    Handle sensibility var

    :param s: string or float containing information of sensibility to detect peak
    :return: float
    """

    try:
        sensibility = float(s)
        if 0 < sensibility <= 1:
            return sensibility
        else:
            raise argparse.ArgumentTypeError("Sensibility must be a number between 0 and 1")
    except:
        raise argparse.ArgumentTypeError("Sensibility must be a number between 0 and 1")

def set_min_tweet_per_sec(s):
    """
    Handle tweets per second values

    :param s: string or float containing information of threshold
    :return: float
    """

    try:
        tweets_per_sec = float(s)
        return tweets_per_sec
    except:
        raise argparse.ArgumentTypeError("Tweets per second must be a number")

def set_time_frame(s):
    """
    Handles time frame values. It will be the input of the pandas function s.resample:
     Ex:
        s.resample('2s') -- Group by sets of 2 seconds
        s.resample('5min') -- Group by sets of 5 minutes
        s.resample('1hour') -- Group by sets of one hour

    :param s: string containing information of time frame size
    :return: s if it's on the right format
    """

    try:

        frames = ['s', 'min', 'hour']
        # test to check if s is in the correct format
        match = re.match(r"([0-9]+)([a-z]+)", s, re.I)
        items = match.groups()

        if items[0].isdigit():
            if items[1].lower() in frames:
                return s.lower()
            else:
                raise argparse.ArgumentTypeError(
                    """time unit must be one of those options: s, min or hour""")

    except:
        raise argparse.ArgumentTypeError(
             """Input must contain a positive integer and the time unity. Ex: 30s, 5min or 1hour""")

def set_sample_size(s):
    """
    Handle dataset size values

    :param s: string or integer
    :return: int
    """

    try:
        size = int(s)
        return size
    except:
        raise argparse.ArgumentTypeError("dataset size must be an integer")