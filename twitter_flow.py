from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import sqlite3 as sql
from datetime import datetime
import logging
import sys


# Set log config
logging.basicConfig(filename='log/twitter_flow.txt', level=logging.DEBUG)
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
#logger.addHandler(handler)

class StdoutListener(StreamListener):
    """
    Listen to twitter stream and write to a database.
    """
    def __init__(self):
        super().__init__()
        # Create connection
        self.conn = sql.connect('./twitter_streaming_data.db')

    def on_data(self, data):
        try:
            # Loads twitter's json data into a data base
            received_tweet = json.loads(data)
            tweet_id = received_tweet["id"]
            created_at = received_tweet["created_at"]
            tweet_hashtags = [x["text"] for x in received_tweet["entities"]["hashtags"]]

            # Start to populate tweet table
            infos = []
            for hashtag in tweet_hashtags:

                logger.debug('Found hashtag %s', hashtag)
                tweet_info = [tweet_id, str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")), created_at, hashtag]
                infos.append(tweet_info)

            query = """insert into tweets(tweet_id, insert_date, created_at, hashtag) values(?, ?, ?, ?);"""
            with self.conn:
                self.conn.executemany(query, infos)

            return True

        except Exception as e:

            logger.exception(e)

    def on_error(self, status):
        logger.debug('on_error %s', status)


if __name__ == '__main__':
    # set Twitter access
    logger.info('Setting keys')
    consumer_key = 'VJNTaFy9k8wOhvLMCNMrdrJ5b'
    consumer_secret_key = 'TlyJ8hObmwTxXrbOmg0qXI0AO65FgwpDPuiw1lXJtLjuirThEF'
    access_token = '780782501551747072-NGmaIuimHtagKga83PQjk575MSg2Mfq'
    access_secret_token = 'Zi6ma6rHNPjm915qQvhwy4UjTw0c4CbQHKeVSsL7gjpuM'

    logger.info('Initializing listener')
    # Instantiate listener
    l = StdoutListener()

    logger.info('Authorization')
    # Set authentication
    auth = OAuthHandler(consumer_key, consumer_secret_key)
    auth.set_access_token(access_token, access_secret_token)

    logger.info('Beginning streaming')
    # Start data stream
    stream = Stream(auth, l)
    stream.filter(track=['SundayMorning'], languages=['en'])