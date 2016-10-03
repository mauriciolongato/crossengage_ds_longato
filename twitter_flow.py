from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import sqlite3 as sql
import time


class StdoutListener(StreamListener):

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
            # Create connection
            conn = sql.connect('./twitter_streaming_data.db')
            # because some tweets has more the one hashtag
            # print(tweet_hashtags)
            infos = []
            for hashtag in tweet_hashtags:

                # Here we are inserting twitter's information in our database
                tweet_info = [tweet_id, str(time.strftime("%d/%m/%Y %H:%M:%S")), created_at, hashtag]
                # print(tweet_info)
                infos.append(tweet_info)

            conn.executemany(
                """insert into tweets(tweet_id, insert_date, created_at, hashtag) values(?, ?, ?, ?);""", infos)
            conn.commit()
            conn.close()

            return True

        except Exception:
            # Create a log of the error and the lost twitter
            # set a log
            print("deu ruim")
            conn.close()


    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # set Twitter access
    consumer_key = 'VJNTaFy9k8wOhvLMCNMrdrJ5b'
    consumer_secret_key = 'TlyJ8hObmwTxXrbOmg0qXI0AO65FgwpDPuiw1lXJtLjuirThEF'
    access_token = '780782501551747072-NGmaIuimHtagKga83PQjk575MSg2Mfq'
    access_secret_token = 'Zi6ma6rHNPjm915qQvhwy4UjTw0c4CbQHKeVSsL7gjpuM'

    # Instantiate listener
    l = StdoutListener()

    # Set authentication
    auth = OAuthHandler(consumer_key, consumer_secret_key)
    auth.set_access_token(access_token, access_secret_token)

    # Start data stream
    stream = Stream(auth, l)
    stream.filter(track=['obama'], languages=['en'])