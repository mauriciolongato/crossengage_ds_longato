import argparse
import multiprocessing as mp
import uuid

import twitter_analyser
import twitter_flow
from helpers import DbFunctions
from helpers.input_specification import location_coord, track_list, set_sensibility, set_min_tweet_per_sec, \
    set_time_frame, set_sample_size


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
    db_obj = DbFunctions.DbFunctions(db_name)
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

    parser.add_argument('--analysis_sample_size',
                        help="How many frames will be considered in order to analyse peak existence",
                        dest="analysis_sample_size",
                        type=set_sample_size,
                        nargs=1)

    args = parser.parse_args()

    #@TODO: Shouldn't be hardcoded
    consumer_key = 'VJNTaFy9k8wOhvLMCNMrdrJ5b'
    consumer_secret_key = 'TlyJ8hObmwTxXrbOmg0qXI0AO65FgwpDPuiw1lXJtLjuirThEF'
    access_token = '780782501551747072-NGmaIuimHtagKga83PQjk575MSg2Mfq'
    access_secret_token = 'Zi6ma6rHNPjm915qQvhwy4UjTw0c4CbQHKeVSsL7gjpuM'

    # 3. Initialize twitter_flow
    languages=[['en']]

    flow_params = [consumer_key,
                   consumer_secret_key,
                   access_token,
                   access_secret_token,
                   languages,
                   args.track,
                   args.locations,
                   db_obj]

    analyser_params =[args.peak_detection_sensibility,
                      args.minimum_tweet_per_sec,
                      args.time_frame,
                      args.analysis_sample_size,
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
