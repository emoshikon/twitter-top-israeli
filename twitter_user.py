#!/usr/bin/env python
# encoding=utf8

import io
import tweepy
from pickle import Pickler, Unpickler
from collections import deque, Counter
from datetime import datetime
from time import sleep
import json
import sys
import numpy

class TwitterUser:
    VERSION = '0.5'
    STATUSES_BACK = 20
    HEBREW_BAR = int(0.3 * STATUSES_BACK)
    MIN_FOLLOWERS = 2000
    FIND_FOLLOWS = True
    api = None
    logfh = sys.stdout
    def __init__(self, id_str, save_func=None):
        '''
        Variables:
            id_str - Twitter user ID
            save_func - a function to execute when 
            follows - a set of all the people this user is following
            screen_name - the alias for this user
            name - full name of this user
            friends_count - the number of people this user is following, as given by the API
            followers_count - the number of people follwing this user
            total_retweet_count - how many retweets this user achieved in the last $STATUSES_BACK
        Flags:
            is_tiny - less than $MIN_FOLLOWERS so no worth spending time and capacity
            is_raeli - writes hebrew posts
            is_active - wrote at least 12 posts since 2015
            is_protected - doesn't share tweets publicly
        Methods:
            contains_hebrew() - a helper function to check for hebrew letters
        ''' 
        self.id_str = id_str
        self.save_func = save_func
        self.follows = set() #[u.screen_name for u in user_obj.friends()]
        self.total_retweet_count = 0
        self.retweets_95p = 0
        self.total_heb_retweet_count = 0
        self.heb_retweets_95p = 0
        self.failed = False
        self.is_tiny = False
        self.is_raeli = False
        self.is_active = False
        self.is_protected = False
        self.count_tweets = 0
        self.count_heb_tweets = 0
        user_obj = None
        retweets_list = list()
        retweets_heb_list = list()
        for tweet in self.limit_handled(tweepy.Cursor(self.api.user_timeline, id=id_str, include_rts=False, exclude_replies=True, count=200).items()):
            if tweet.user.followers_count < self.MIN_FOLLOWERS:
                self.is_tiny = True
                break
            if self.count_heb_tweets == self.HEBREW_BAR:
                self.is_raeli = True
                user_obj = tweet.user
            heb_tweet = False
            if tweet.lang == 'he':
                self.count_heb_tweets += 1
                heb_tweet = True
            else:
                if self.contains_hebrew(tweet.text):
                    self.count_heb_tweets += 1
                    heb_tweet = True
            if heb_tweet:
                self.total_heb_retweet_count += tweet.retweet_count
                retweets_heb_list.append(tweet.retweet_count)
            self.count_tweets += 1
            if self.count_tweets == 11: # the 12th tweet need to be less than a year old
                if tweet.created_at.year > 2015:
                    self.is_active = True
            self.total_retweet_count += tweet.retweet_count
            retweets_list.append(tweet.retweet_count)
            if ((self.count_heb_tweets == self.STATUSES_BACK) | (self.count_tweets > self.STATUSES_BACK + self.HEBREW_BAR)):
                break
        self.retweets_95p = numpy.percentile(retweets_list,95)
        self.retweets_heb_95p = numpy.percentile(retweets_heb_list,95)
        if self.is_raeli & self.is_active:
            self.screen_name = unicode(user_obj.screen_name)
            self.name = unicode(user_obj.name) or ''
            self.followers_count = int(user_obj.followers_count) 
            self.friends_count = int(user_obj.friends_count)
            if self.FIND_FOLLOWS:
                for user_id in self.limit_handled(tweepy.Cursor(self.api.friends_ids, id=id_str).items()):
                    self.follows.add(str(user_id))

    def __repr__(self):
        return (u"Name: %s\nFollowing: %d\nTable: %s" % (self.name,self.friends_count,vars(self))).encode('utf8')
 
    @staticmethod
    def contains_hebrew(tweet):
        if any(u"\u0590" <= c <= u"\u05EA" for c in tweet):
            return True
        return False

    def limit_handled(self,cursor):
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                self.handle_pause()
            except tweepy.TweepError as e:
                if hasattr(e.response, 'status_code'):                    
                    if e.response.status_code == 401:
                        self.is_protected = True
                        break
                    elif e.response.status_code == 429:
                        self.handle_pause()
                else:
                    self.logfh.write(u" - Problem with user %s due to %s\n" % (self.id_str,e))
                    self.failed = True
                    break
            except not tweepy.TweepError as e:
                self.logfh.write(u"!! SOMETHING HAPPENED: %s\n" % e)
                self.save_handler(self.id_str)
                self.logfh.write(u"%s - Unexpectaly quit digging into Twitter\n" % (datetime.now()))
                raise

    def handle_pause(self):
        self.logfh.write(u"\n\t\tRate limited so paused user %s\n" % self.id_str)
        rate = self.api.rate_limit_status()
        friends_ids = rate['resources']['friends']['/friends/ids']['remaining']
        user_timeline = rate['resources']['statuses']['/statuses/user_timeline']['remaining']
        friend_reset = rate['resources']['friends']['/friends/ids']['reset']
        friend_reset_time = datetime.fromtimestamp(friend_reset)
        status_reset = rate['resources']['statuses']['/statuses/user_timeline']['reset']
        status_reset_time = datetime.fromtimestamp(status_reset)
        self.logfh.write(u"\t\tWaiting for rate limit at %s - /friends/ids=%d (->%s) & /statuses/user_timeline=%d (->%s)\n" % (datetime.now().time().replace(microsecond=0).isoformat(),friends_ids,friend_reset_time.time().isoformat(),user_timeline,status_reset_time.time().isoformat()))
        self.save_func(self.id_str)
        self.logfh.write(u"\t\t\tsleeping ")
        char = u'/'
        while True:
            self.logfh.write(char)
            self.logfh.flush()
            try:
                sleep(15)
            except (KeyboardInterrupt, SystemExit):
                self.logfh.write(u"\n\t\tAdmin interrupted the script. Trying to save..\n")
                self.save_handler(self.id_str)
                self.logfh.write(u"%s - Quit digging into Twitter\n" % (datetime.now()))
                exit()
            if friend_reset_time < datetime.now():
                self.logfh.write(u"\n\tResuming ID %s" % self.id_str)
                break
            char = u'\\' if char == u'/' else u'/'
        return True

if __name__ == '__main__':
    log = io.open('progress.log', 'a',buffering=1,encoding='utf8')
    log.write(u"\n%s - Started digging into Twitter\n" % (datetime.now()))
    def save(user_id=None):
        stored_queue = deque(queue)
        if user_id:
            stored_queue.appendleft(user_id)
        with open('db/tweeters.db','wb') as f:
            Pickler(f,-1).dump(users)
        with open('db/queue.db','wb') as f:
            Pickler(f,-1).dump(stored_queue)
        with open('db/scores.db','wb') as f:
            Pickler(f,-1).dump(scores)
        for s in sets.keys():
            with io.open('db/%s.list' % s , 'w' , encoding='utf8') as f:
                f.write(u','.join(sets[s]))
        log.write(u"\t\tSaved: Queue = %d | Done = %d | Not-Israeli = %d | Not-Active = %d | Tiny = %d\n" % (len(stored_queue),len(sets['done']),len(sets['not_israeli']),len(sets['not_active']),len(sets['tiny'])))

    BASE_USER_SCREEN_NAME = 'amit_segal'
    BASE_USER_ID = '114894966'
    
    with io.open('tokens.json','r',encoding='utf8') as f:
        tokens = json.load(f)
    auth = tweepy.OAuthHandler(tokens['consumer_key'], tokens['consumer_secret'])
    auth.set_access_token(tokens['access_token'], tokens['access_token_secret'])
    TwitterUser.api = tweepy.API(auth)
    TwitterUser.logfh = log
    users = {}
    queue = deque([BASE_USER_ID])
    scores = Counter()
    sets = {
        'not_israeli' : None,
        'not_active' : None,
        'protected' : None,
        'done' : None,
        'tiny' : None
    }

    with open('db/tweeters.db','r') as f:
        users = Unpickler(f).load()
    with open('db/queue.db','r') as f:
        queue = Unpickler(f).load()
    with open('db/scores.db','r') as f:
        scores = Unpickler(f).load()
    for s in sets.keys():
        with io.open('db/%s.list' % s , 'r' , encoding='utf8') as f:
            content = f.read()
            sets[s] = set() if content == '' else set(map(str, content.split(',')))
    skips = sets['protected'] | sets['not_israeli'] | sets['not_active'] | sets['tiny']

    log.write(u"\tLoading: Queue = %d | Done = %d | Not-Israeli = %d | Not-Active = %d | Tiny = %d\n\t" % (len(queue),len(sets['done']),len(sets['not_israeli']),len(sets['not_active']),len(sets['tiny'])))
    jumped = False
    while len(queue):
        user_id = queue.popleft()
        if user_id in skips:
            jumped = True
            log.write(u'-')
            log.flush()
            continue
        if user_id in sets['done']:
            jumped = True
            scores[user_id] += 1
            log.write(u'+')
            log.flush()
            continue
        if jumped:
            log.write(u"\n\t")
            jumped = False
        log.write(u"Pulling ID %s" % user_id)
        user = TwitterUser(user_id,save)
        if user.failed:
            queue.append(user_id)
        elif user.is_protected:
            sets['protected'].add(user_id)
            skips.add(user_id)
            log.write(u" - oh, tweets are protected\n\t")
        elif user.is_tiny:
            sets['tiny'].add(user_id)
            skips.add(user_id)
            log.write(u" - neahh, less than %d followers\n\t" % user.MIN_FOLLOWERS)
        elif not user.is_raeli:
            sets['not_israeli'].add(user_id)
            skips.add(user_id)
            log.write(u" - oh, not Israeli!\n\t")
        elif not user.is_active:
            sets['not_active'].add(user_id)
            skips.add(user_id)
            log.write(u" - oh, not Active!\n\t")
        else:
            users[user_id] = user
            queue.extend(user.follows)
            scores[user_id] = 1
            sets['done'].add(user_id)
            log.write(u" - It's %s (@%s)\n\t" % (user.name,user.screen_name))
    save()
    log.write(u"%s - Queue is empty, done digging into Twitter\n" % (datetime.now()))
#https://tweeterid.com/
