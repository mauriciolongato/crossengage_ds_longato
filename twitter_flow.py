from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from datetime import datetime
import logging

import configkeys as ck
from helpers.DbFunctions import DbFunctions
from helpers.data_type import set_datetime_format


# Set log config

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler and ser level to debug
fh = logging.FileHandler('log/twitter_flow.txt')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)


class StdoutListener(StreamListener):
    """
    Listen to twitter stream and write to a database.
    """

    def __init__(self, db_obj):
        super().__init__()
        self.db_obj = db_obj

    def on_data(self, data):

        try:
            # Loads json and parsing data
            received_tweet = json.loads(data)

            # get tweet id
            tweet_id = received_tweet["id"]

            # get twitter posting date
            created_at = received_tweet["created_at"]

            # list all hashtags from that tweet
            tweet_hashtags = [x["text"] for x in received_tweet["entities"]["hashtags"]]


            # Start to populate tweet table
            info = []
            for hashtag in tweet_hashtags:

                logger.debug('Found hashtag %s', hashtag)

                tweet_info = (tweet_id,
                              datetime.utcnow().isoformat(),
                              set_datetime_format(created_at).replace(tzinfo=None).isoformat(),
                              hashtag)

                info.append(tweet_info)

            self.db_obj.insert_into_tweets(info)

            return True

        except Exception:
            logger.exception("message")


    def on_error(self, status):
        logger.debug('on_error %s', status)


def start_flow(consumer_key, consumer_secret_key,
               access_token, access_secret_token,
               languages, track, locations, data_base):
    """
    Interface between defining parameter (main.py) and instantiate StdoutListener class
    """

    logger.info('Initializing listener')
    # Instantiate listener
    l = StdoutListener(data_base)

    logger.info('Authorization')
    auth = OAuthHandler(consumer_key, consumer_secret_key)
    auth.set_access_token(access_token, access_secret_token)

    # Start data stream
    logger.info('Beginning streaming')
    stream = Stream(auth, l)
    stream.filter(track=track[0],
                  languages=languages[0],
                  locations=locations[0])


if __name__ == '__main__':
    # set Twitter access
    logger.info('Setting keys')

    # Get twitter keys
    consumer_key = ck.keys["consumer_key"]
    consumer_secret_key = ck.keys['consumer_secret_key']
    access_token = ck.keys['access_token']
    access_secret_token = ck.keys['access_secret_token']

    name ='./example_db/teste_set'
    db_obj = DbFunctions(name)
    db_obj.set_tables()

    locations = [(-79.762418, 40.477408, -71.778137, 45.010840)]
    start_flow(consumer_key, consumer_secret_key, access_token, access_secret_token, [['trump', 'obama']], locations, db_obj)