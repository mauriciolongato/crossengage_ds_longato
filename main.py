import argparse
import multiprocessing as mp
import uuid
import re

import twitter_analyser
import set_db
import twitter_flow
import flask_app



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
                raise argparse.ArgumentTypeError("Coordinates must be at least 4 values")
        else:
            raise argparse.ArgumentTypeError("Coordinates must be sw_lon, sw_lat, ne_lon, ne_lat")
    except:
        raise argparse.ArgumentTypeError("Coordinates must be sw_lon, sw_lat, ne_lon, ne_lat")

def track_list(s):
    """
    Handle topic list

    :param s: string containing a words separated by comma
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
        if sensibility > 0 and sensibility <=1:
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
    Handle time frame values values. It will be the input of a pandas function:
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

        #@TODO: finish errors handlers
        if len(items) != 2:
            raise
                #argparse.ArgumentTypeError(
                #"""Input must contain a positive integer and the time unity. Ex: 30s, 5min or 1hour""")
        else:
            if items[0].isdigit():
                if items[1].lower() in frames:
                    return s.lower()
                else:
                    """time unity is not correctly specified"""
                    raise
                        #argparse.ArgumentTypeError(
                        #"""Time unity must be one of those options: s, min or hour""")
            else:
                """first item is not a digit"""
                raise
                    #argparse.ArgumentTypeError(
                    #"""The number should be a positive integer""")

        # items(1) should be one of those values ['s', 'min', 'hour']
        # Must contain one of those values for the time frame

    except:
        raise argparse.ArgumentTypeError(
             """Input must contain a positive integer and the time unity. Ex: 30s, 5min or 1hour""")

def dataset_size(s):
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

def main():
    """
    Initialize processes:
        1. Create DB
        2. Handle parameters
        3. Initialize twitter_flow
        4. Initialize twitter_analyzer
        5. Initialize flask_app

    The periodicity of twitter_analyser is a multiple of time_frame

    """

    # 1. Create DB
    db_name = uuid.uuid1()
    db_obj = set_db.instance_db(db_name)
    db_obj.set_tables()

    # 2. Handle parameters
    parser = argparse.ArgumentParser(description='Run peak detector.')

    parser.add_argument('--track',
                        help="Track ex: 'word_1, word_2, ... word_n'",
                        dest="track",
                        type=track_list,
                        nargs=1)

    parser.add_argument('--loc',
                        help="Coordinate 'sw_lon_1, sw_lat_1, ne_lon_1, ne_lat_1, ...'",
                        dest="locations",
                        type=location_coord,
                        nargs=1)

    parser.add_argument('--sensibility',
                        help="Peak detection sensibility - number between 0 and 1",
                        dest="peak_detection_sensibility",
                        type=set_sensibility,
                        nargs=1)

    parser.add_argument('--min_tweets_sec',
                        help="Minimum tweets per second",
                        dest="minimum_tweet_per_sec",
                        type=set_min_tweet_per_sec,
                        nargs=1)

    parser.add_argument('--time_frame',
                        help="Time frame - tweets will be grouped in this time frames",
                        dest="time_frame",
                        type=set_time_frame,
                        nargs=1)

    parser.add_argument('--analysis_dataset_size',
                        help="How many frames will be considered in order to analyse peak existence",
                        dest="analysis_dataset_size",
                        type=dataset_size,
                        nargs=1)

    args = parser.parse_args()

    print(args)


    #@TODO: Shouldn't be hardcoded
    consumer_key = 'VJNTaFy9k8wOhvLMCNMrdrJ5b'
    consumer_secret_key = 'TlyJ8hObmwTxXrbOmg0qXI0AO65FgwpDPuiw1lXJtLjuirThEF'
    access_token = '780782501551747072-NGmaIuimHtagKga83PQjk575MSg2Mfq'
    access_secret_token = 'Zi6ma6rHNPjm915qQvhwy4UjTw0c4CbQHKeVSsL7gjpuM'

    # 3. Initialize twitter_flow
    flow_params = [consumer_key,
                   consumer_secret_key,
                   access_token,
                   access_secret_token,
                   args.track,
                   args.locations,
                   db_obj]

    analyser_params =[args.peak_detection_sensibility,
                      args.minimum_tweet_per_sec,
                      args.time_frame,
                      args.analysis_dataset_size,
                      db_obj]


    flow_process = mp.Process(target=twitter_flow.start_flow, args=flow_params)
    flow_process.start()

    analyzer_process = mp.Process(target=twitter_analyser.start_analyzer, args=analyser_params)
    analyzer_process.start()

    #flask_app = mp.Process(target=flask_app.start_analyzer, args=analyser_params)
    #flask_app.start()
    #flask_app.join()

    flow_process.join()
    analyzer_process.join()


if __name__ == '__main__':
    # Test_config = --track "trump, obama"
    #               --loc "-79.762418, 40.477408, -71.778137, 45.010840, -79.762418, 40.477408, -71.778137, 45.010840"
    #               --sensibility 0.98
    #               --time_frame "1Min"
    #               --min_tweets_sec 0.1

    main()
