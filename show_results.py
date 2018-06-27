#!/usr/bin/env python
# encoding=utf8
activate_this = '/Users/molevin/git/twitter-top-israeli/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

import io
import tweepy
from pickle import Pickler, Unpickler
from collections import Counter
from datetime import datetime
import json
import sys
from twitter_user import TwitterUser

def save():
    pass
with open('db/tweeters.db','r') as f:
    users = Unpickler(f).load()
with open('db/queue.db','r') as f:
    queue = Unpickler(f).load()
with open('db/scores.db','r') as f:
    scores = Unpickler(f).load()
# print users['847263877'].total_retweet_count
# exit()
print "\t".join(['id','screen-name','name','score','followers','retweets'])
for user in scores:
    info = users[user]
    score = scores[user]
    print "\t".join([user,info.screen_name.encode('utf8'),info.name.encode('utf8'),str(score),str(info.followers_count),str(info.total_retweet_count)])