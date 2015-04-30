# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 19:46:41 2015

@author: rep
"""
from __future__ import absolute_import, print_function

from datetime import datetime
import sqlite3
db = sqlite3.connect('data/tweetdb')
db.row_factory = sqlite3.Row
curs = db.cursor()


create_tweet_table = '''

CREATE TABLE IF NOT EXISTS tweet_table
(
    tweetId INTEGER PRIMARY KEY,
    tweetText TEXT,
    userId INTEGER,
    userName TEXT,
    geom TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP

)'''
create_user_table = '''
CREATE TABLE IF NOT EXISTS user_table
(
    userId INTEGER,
    userName TEXT,
    tweetCount INTEGER,
    lastContact TIMESTAMP,
    firstContact TIMESTAMP,
    numContact   TIMESTAMP,
    contacted BOOLEAN,
    imgUrl TEXT
    
)
'''

create_index = '''
CREATE INDEX IF NOT EXISTS twt_created_idx ON tweet_table(created_at); 
CREATE INDEX IF NOT EXISTS twt_user_id_idx ON tweet_table(userId);
CREATE INDEX IF NOT EXISTS twt_tweetId_idx ON tweet_table(tweetId);


CREATE INDEX IF NOT EXISTS usr_lastContact_idx ON user_table(lastContact); 
CREATE INDEX IF NOT EXISTS usr_user_id_idx ON user_table(userId);
CREATE INDEX IF NOT EXISTS usr_contacted_idx ON user_table(contacted); 
CREATE INDEX IF NOT EXISTS usr_numContact_idx ON user_table(numContact); 
CREATE INDEX IF NOT EXISTS usr_tweetCount_idx ON user_table(tweetCount);
CREATE INDEX IF NOT EXISTS usr_firstContact_idx ON user_table(firstContact); 


'''
curs.execute(create_tweet_table)
curs.execute(create_user_table)
curs.executescript(create_index)

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pprint import pprint


import folium
map_osm = folium.Map(location=[45.5236, -122.6750])
map_osm.create_map(path='osm.html')
import json
class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        
        data =json.loads(data)
        db = sqlite3.connect('data/tweetdb')
        db.row_factory = sqlite3.Row
        curs = db.cursor()
        curs.execute('''INSERT INTO tweet_table(tweetId, tweetText, userID, userName,geom)
                  VALUES(?,?,?,?,?)''', (data['id']
                  ,data['text'], data['user']['id'], data["user"]["screen_name"],str(data['geo'])))
                  
        curs.execute('''INSERT INTO user_table(userID, userName, imgUrl)
                  VALUES(?,?,?)''', (data['user']['id'], data["user"]["screen_name"],data['user']['profile_image_url']))                  
                  
        curs.close()
        db.commit()
        return True

    def on_error(self, status):
        print(status)



    

if __name__ == '__main__':
    consumer_key = "94OGxBVHmEKrk94HVouSHmxnF"
    consumer_secret = "bO6havdbWyBN4AY7TMbcVzxdfIsiVkTANZvDu3TBrSEeOhe8Dt"
    access_token = "1453200823-H0XVYswdQYsNO7zDlJN7zLWI849712Mo3xPraqq"
    access_token_secret = "vbNUJdUFPiiMSUnePgn8KcqoKyQH0dOKvYLkPlX78lBYL"
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(locations=[-73.5752,45.4901,-73.5614,45.502], async=False)







