import sqlite3 as sql
import os
import pandas as pd
import logging


# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler and ser level to debug
fh = logging.FileHandler('log/DbFunctions.txt')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)


class DbFunctions:
    """
    Handles twitter database functions
    """

    def __init__(self, name):
        """
        :param name: name from the DB that will store the data
        """
        self.name = name
        self.address = "./databases"

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
            except Exception:
                logger.exception("fail to insert peak data: {}".format(peak_list_info))

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

    def get_last_tweet_peak(self):

        query = "select * from tweet_peaks ORDER by peak_datetime desc limit 1"
        with sql.connect('./{}.db'.format(self.name)) as conn:
            proc_data = conn.execute(query)
            peak = proc_data.fetchall()

        try:
            peak_dict = dict(list(peak)[0])
            return peak_dict

        except Exception:

            logger.exception("fail to get a peak data: {}".format(peak_dict))


class DbRequests:
    """
    Handles requests_manager database functions
    """

    def __init__(self):
        """
        :param name: name from the DB that will store the data
        """
        self.name = "request_manager"

    def set_requests_table(self):
        """
        Create all the necessary tables to store requests data
        """
        with sql.connect('./{}.db'.format(self.name)) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS requests(
                            request_id INTEGER PRIMARY KEY,
                            created_at TEXT,
                            track_list TEXT,
                            languages TEXT,
                            locations TEXT,
                            minimum_tweet_per_sec TEXT,
                            time_frame TEXT,
                            peak_detection_sensibility TEXT,
                            analysis_sample_size TEXT,
                            db_tweets_name TEXT)
                        """)

    def insert_into_request_table(self, request_list):
        """
        Handle with insertion into tweet_peaks table
        """
        query = """insert into
                    requests(created_at, track_list, languages, locations, minimum_tweet_per_sec,
                    time_frame, peak_detection_sensibility, analysis_sample_size, db_tweets_name)
                    values(?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        with sql.connect('./{}.db'.format(self.name)) as conn:
            try:
                conn.execute(query, request_list)
            except Exception:
                logger.exception("fail to insert peak data: {}".format(request_list))

    def get_last_execution(self):
        query = "select db_tweets_name from requests order by request_id desc limit 1;"
        with sql.connect('./{}.db'.format(self.name)) as conn:
            proc_data = conn.execute(query)
            name = proc_data.fetchall()

        return name[0][0]