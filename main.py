import argparse
import multiprocessing as mp
import twitter_analyser
import twitter_flow


def main():
    parser = argparse.ArgumentParser(description='Run peak detector.')
    parser.add_argument('track')
    parser.add_argument('locations')
    parser.add_argument('languages')

    args = parser.parse_args()

    consumer_key = 'VJNTaFy9k8wOhvLMCNMrdrJ5b'
    consumer_secret_key = 'TlyJ8hObmwTxXrbOmg0qXI0AO65FgwpDPuiw1lXJtLjuirThEF'
    access_token = '780782501551747072-NGmaIuimHtagKga83PQjk575MSg2Mfq'
    access_secret_token = 'Zi6ma6rHNPjm915qQvhwy4UjTw0c4CbQHKeVSsL7gjpuM'

    flow_params = [consumer_key,
                   consumer_secret_key,
                   access_token,
                   access_secret_token,
                   args.track,
                   args.location,
                   args.languages]

    flow_process = mp.Process(target=twitter_flow.start_flow, args=flow_params)
    flow_process.start()

    analyzer_process = mp.Process(target=twitter_analyser.start_analyzer, args=[])
    analyzer_process.start()

    flow_process.join()
    analyzer_process.join()


if __name__ == '__main__':
    main()
