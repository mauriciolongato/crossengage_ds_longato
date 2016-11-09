import sqlite3 as sql
import os
import pandas as pd


class DbFunctions:
    """
    Handles twitter database functions
    """

    def __init__(self, name):
        """
        :param name: name from the DB that will store the data
        """
        self.name = name

    def drop_tables(self):
        """
        Drop all the tables from the instantiated DB
        """
        with sql.connect('./{}.db'.format(self.name)) as conn:
            conn.execute("DROP TABLE tweets")
            conn.execute("DROP TABLE tweet_peaks")

    def set_tables(self):
        """
        Create all the necessary tables to store twitter streaming data and analysis
        """
        with sql.connect('./{}.db'.format(self.name)) as conn:
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
        current_dir = os.getcwd()
        os.remove(current_dir + "/" + str(self.name) + ".db")

    def insert_into_tweets(self, infos):
        """
        Handle with insertion into tweet table
        """
        query = "insert into tweets(tweet_id, insert_date, created_at, hashtag) values(?, ?, ?, ?);"
        with sql.connect('./{}.db'.format(self.name)) as conn:
            conn.executemany(query, infos)

    def insert_into_tweet_peaks(self, peak_list_info):
        """
        Handle with insertion into tweet_peaks table
        """
        query = """insert into
                    tweet_peaks(id, peak_datetime, time_frame, hashtag, mean, std, sensibility
                                , freq_limit, qt_tweets, probability)
                    values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        with sql.connect('./{}.db'.format(self.name)) as conn:
            try:
                conn.executemany(query, peak_list_info)
            except Exception as e:
                # @TODO: Set log here!
                # logger.exception(e)
                pass

    def get_tweets_data(self):
        """
        get the data from tweets table
        :return: pandas dataframe with data
        """
        query = "select * from tweets;"
        with sql.connect('./{}.db'.format(self.name)) as conn:
            proc_data = conn.execute(query)
            data = proc_data.fetchall()

        cols = ["id", "tweet_id", "insert_date", "created_at", "hashtag"]
        tweets = pd.DataFrame.from_records(data=data, columns=cols)

        return tweets

    def get_tweets_data_interval(self, time_i, time_f):
        """

        :param time_i:
        :param time_f:
        :return:
        """

        query = "select * from tweets where created_at between '{}' and '{}';".format(time_i, time_f)

        with sql.connect('./{}.db'.format(self.name)) as conn:
            proc_data = conn.execute(query)
            data = proc_data.fetchall()

        cols = ["id", "tweet_id", "insert_date", "created_at", "hashtag"]
        tweets = pd.DataFrame.from_records(data=data, columns=cols)

        return tweets

    def get_time_last_tweet_tweets(self):

        query = "select created_at from tweets order by 1 desc limit 1;"
        with sql.connect('./{}.db'.format(self.name)) as conn:
            proc_data = conn.execute(query)
            time = proc_data.fetchall()

        return time[0][0]