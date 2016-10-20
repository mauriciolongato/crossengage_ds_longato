import sqlite3 as sql
import os


class instance_db:

    def __init__(self, name):
        """
        Class handles with the data storage from the twitter
        :param name: name from the DB that will store the data
        """
        self.name = name
        self.conn = sql.connect('./{}.db'.format(self.name))

    def drop_tables(self):
        """
        Drop all the tables from the instantiated DB
        """
        with self.conn as conn:
            conn.execute("drop table tweets")
            conn.execute("drop table tweet_peaks")

    def set_tables(self):
        """
        Create all the necessary tables to store twitter streaming data and analysis
        """
        with self.conn as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS tweets(
                            id INTEGER PRIMARY KEY,
                            tweet_id INTEGER,
                            insert_date TEXT,
                            created_at TEXT,
                            hashtag TEXT)
                        """)

            conn.execute("""CREATE TABLE tweet_peaks(
                            peak_datetime TEXT NOT NULL,
                            hashtag TEXT NOT NULL,
                            time_frame TEXT,
                            mean REAL,
                            std REAL,
                            sensibility REAL,
                            freq_limit REAL,
                            qt_tweets INTEGER,
                            id TEXT PRIMARY KEY,
                            probability REAL);
                        """)

    def delete_db_file(self):
        """
        Delete db file
        """
        dir = os.getcwd()
        os.remove(dir+"/"+str(self.name)+".db")

    def insert_into_tweets(self, infos):
        """
        Handle with insertion into tweet table

        :param infos: list of tweets -  Each tweet contain:

        infos = [(tweet_id, insert_date, created_at, hashtag)]

        :param tweet_id: Twitter's tweet id
        :param insert_date: Current Date time - Greenwich, isoformat
        :param created_at: Twitter's creation date - COnverted to isoformat
        :param hashtag: Tweet's hashtag
        """

        query = """insert into tweets(tweet_id, insert_date, created_at, hashtag)
                   values(?, ?, ?, ?);"""

        with self.conn:
            self.conn.executemany(query, infos)

    def insert_into_tweet_peaks(self, peak_list_info):
        """
        Handle with insertion into tweet_peaks table

        :param infos: list of tweets -  Each tweet contain:

        :param id: peak_datetime + hashtag
        :param peak_datetime: tweet
        :param time_frame:
        :param hashtag:
        :param mean:
        :param std:
        :param sensibility:
        :param freq_limit:
        :param qt_tweets:
        :param probability:
        :return:
        """

        query = """insert into
                    tweet_peaks(id, peak_datetime, time_frame, hashtag, mean, std, sensibility
                                , freq_limit, qt_tweets, probability)
                    values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        with sql.connect('./twitter_streaming_data.db') as conn:
            try:
                conn.executemany(query, peak_list_info)
            except Exception as e:
                logger.exception(e)



