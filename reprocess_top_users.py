#!/usr/bin/env python
# encoding=utf8

import io
from pickle import Pickler, Unpickler
from collections import Counter
from datetime import datetime
import json
import sys
from twitter_user import TwitterUser
import tweepy

if __name__ == '__main__':
    log = io.open('reprocess_progress.log', 'a',buffering=1,encoding='utf8')
    log.write(u"\n%s - Started reprocessing tweeps\n" % (datetime.now()))
    def save(user_id=None):
        pass
    with open('db/scores.db','r') as f:
        scores = Unpickler(f).load()
    with io.open('tokens.json','r',encoding='utf8') as f:
        tokens = json.load(f)
    auth = tweepy.OAuthHandler(tokens['consumer_key'], tokens['consumer_secret'])
    auth.set_access_token(tokens['access_token'], tokens['access_token_secret'])
    TwitterUser.api = tweepy.API(auth)
    TwitterUser.logfh = log
    TwitterUser.STATUSES_BACK = 100
    TwitterUser.FIND_FOLLOWS = False
    
    table_headers = ['id_str','name','screen_name','total_retweet_count','retweets_95p','count_tweets','total_heb_retweet_count','retweets_heb_95p','count_heb_tweets','followers_count']
    print "\t".join(table_headers),
    print "\tscore"
    log.write(u"\tprocessing users: ")
    for user, score in scores.most_common(500):
        log.write(u"%s(%d)," % (user,score))
        log.flush()
        #TODO:reprocess their stats based on more tweets
        repro_user = TwitterUser(user,save)
        for key in table_headers:
            try:
                print unicode(getattr(repro_user,key)).encode('utf8'),
                print "\t",
            except:
                print "<<failed>>\t",
        print score

