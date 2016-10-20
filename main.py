import argparse
import multiprocessing as mp
import uuid
import re
import time

import twitter_analyser
import set_db
import twitter_flow



def location_coord(s):
    """
    Handle coordinates input

    :param s: string containing coordinates
    :return: tuple of coordinates
    """
    try:
        sw_lon, sw_lat, ne_lon, ne_lat = map(float, s.split(','))
        return sw_lon, sw_lat, ne_lon, ne_lat
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
        return s_list[0]
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

def twitter_analyser_periodicity(time_frame, periodicity):
    """
    Evaluate the period in seconds between two executions of twitter_analyser

    :param time_frame: Parameter given by the user to set how to frame time data from twitter
    :param periodicity: How many time frames are necessary to evaluate peak
    :return: period between the executions of twitter_analyser in seconds
    """
    frames = {'s':1, 'min':60, 'hour':3600}

    # test to check if s is in the correct format
    match = re.match(r"([0-9]+)([a-z]+)", time_frame, re.I)
    items = match.groups()

    period = items[0] * frames[items[1]] * periodicity

    return period

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
    db = set_db.instance_db(db_name)
    db.set_tables()

    # Get request parameters
    parser = argparse.ArgumentParser(description='Run peak detector.')

    # Parameters to set twitter_flow
    parser.add_argument('--track',
                        help="Track",
                        dest="track",
                        type=track_list,
                        nargs=1)

    parser.add_argument('--loc',
                        help="Coordinate",
                        dest="locations",
                        type=location_coord,
                        nargs=1)

    # Parameters to find peaks
    parser.add_argument('--sensibility',
                        help="Peak detection sensibility",
                        dest="peak_detection_sensibility",
                        type=set_sensibility,
                        nargs=1)

    parser.add_argument('--tweet_seq',
                        help="Minimum tweets per second",
                        dest="minimum_tweet_per_sec",
                        type=set_min_tweet_per_sec,
                        nargs=1)

    parser.add_argument('--time_frame',
                        help="Time frame",
                        dest="time_frame",
                        type=set_time_frame,
                        nargs=1)


    #@TODO: Set analysis_period
    #parser.add_argument('analysis_period')
    args = parser.parse_args()

    print(args)


    consumer_key = 'VJNTaFy9k8wOhvLMCNMrdrJ5b'
    consumer_secret_key = 'TlyJ8hObmwTxXrbOmg0qXI0AO65FgwpDPuiw1lXJtLjuirThEF'
    access_token = '780782501551747072-NGmaIuimHtagKga83PQjk575MSg2Mfq'
    access_secret_token = 'Zi6ma6rHNPjm915qQvhwy4UjTw0c4CbQHKeVSsL7gjpuM'


    flow_params = [consumer_key,
                   consumer_secret_key,
                   access_token,
                   access_secret_token,
                   args.track,
                   args.locations,
                   db]

    analyser_params =[args.peak_detection_sensibility,
                      args.minimum_tweet_per_sec,
                      args.time_frame,
                      db]




    flow_process = mp.Process(target=twitter_flow.start_flow, args=flow_params)
    flow_process.start()

    time.sleep(120)
    analyzer_process = mp.Process(target=twitter_analyser.start_analyzer, args=analyser_params)
    analyzer_process.start()

    flow_process.join()
    analyzer_process.join()


if __name__ == '__main__':
    main()
