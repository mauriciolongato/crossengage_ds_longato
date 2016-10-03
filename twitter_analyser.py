import sqlite3 as sql
import pandas as pd
#import time
from collections import Counter
from operator import itemgetter
from datetime import datetime


def to_local_time(tweet_created_at):

    format = '%a %b %d %H:%M:%S %z %Y'
    return datetime.strptime(tweet_created_at, format)


conn = sql.connect('./twitter_streaming_data.db')
proc_data = conn.execute("""select * from tweets""")

cols = ["id", "tweet_id", "insert_date", "created_at", "hashtag"]
tweets = pd.DataFrame.from_records(data = proc_data.fetchall(), columns = cols)
conn.close()

tweets['agg_created_at'] = tweets['created_at'].map(lambda x: to_local_time(x))

# hashtag frequency sorted from most frequent to less
# hashtags_occurrence = tweets.groupby('hashtag').agg(["count"])["id"].sort_values(["count"], ascending=False)

# group by hashtag
hashtags_groups = tweets.groupby('hashtag')

# treat each hashtag group
for id_group, group in hashtags_groups:
    count = len(group)

    # check if hashtag is significant
    if count > 1000:
        hashtag_agg_timeframe = group.groupby(to_local_time)


# I will analise the most frequent here
hashtag_created_at = tweets[tweets["hashtag"] == "Obama"]["created_at"]

# Convert into python datetime
hashtag_created_at = [str(to_local_time(x)) for x in hashtag_created_at]
# Group tweets and organizer cronologically
# @ In the Counter, x should be handle in fuction of the specified timeframe
hashtag_frequency = sorted(Counter([x[:15] for x in hashtag_created_at]).items())

# Now, we have to analyse hashtag_frequency in order to find the spikes
print(hashtag_frequency)