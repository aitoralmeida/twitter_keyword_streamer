# -*- coding: utf-8 -*-
"""
Created on Tue Oct 28 15:08:20 2014

@author: aitor
"""
import datetime
import json
import gzip
import time
import sys

import tweepy
from tweepy import StreamListener, Stream

credentials = json.load(open('credentials.json', 'r'))
CONSUMER_KEY = credentials['consumer_key']
CONSUMER_SECRET = credentials['consumer_secret']
OAUTH_TOKEN = credentials['oauth_token']
OAUTH_TOKEN_SECRET = credentials['oauth_secret']

tweets = []
initial_time = time.time()


class StdOutListener(StreamListener):       
 
    def on_data(self, raw_data):
        global tweets, initial_time
        
        elapsed_time = time.time () - initial_time #elapsed secons
        #save the status every 30 mins
        if elapsed_time >= 60 * 30:
            now = datetime.datetime.now()
            file_name = './tweets/tweets-%s-%s-%s-%s-%s.txt.gz' % (now.month, now.day, now.hour, now.minute, now.second)
            print '(%s-%s %s:%s:%s) %s' % (now.month, now.day, now.hour, now.minute, now.second, 'Saving file')
            
            with gzip.open(file_name, 'w') as f:
                for tweet in tweets:
                    f.write(json.dumps(tweet) + '\n')
                    
            tweets = []
            initial_time = time.time()
        
        try:
            data = json.loads(raw_data)
            tweets.append(data) 
        except:
            now = datetime.datetime.now()
            print '(%s-%s %s:%s:%s) Invalid JSON Data %s' % (now.month, now.day, now.hour, now.minute, now.second, raw_data)
        
        sys.stdout.flush()
        return True
 
    def on_error(self, status_code):
        now = datetime.datetime.now()
        print '(%s-%s %s:%s:%s)Got an error with status code: %s' % (now.month, now.day, now.hour, now.minute, now.second, status_code)
        sys.stdout.flush()        
        #sleep 5 mins if an error occurs
        time.sleep(5 * 60)
        return True # To continue listening
 
    def on_timeout(self):
        now = datetime.datetime.now()
        print '(%s-%s %s:%s:%s) %s' % (now.month, now.day, now.hour, now.minute, now.second, 'Timeout') 
        sys.stdout.flush()
        return True # To continue listening
                  
 
if __name__ == '__main__':
    now = datetime.datetime.now()
    print '(%s-%s %s:%s:%s) %s' % (now.month, now.day, now.hour, now.minute, now.second, 'Starting') 
    sys.stdout.flush()
    keywords = json.load(open('keywords.json', 'r'))
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    
    listener = StdOutListener()
    
    stream = Stream(auth, listener)
    while True:
        try:
            # https://dev.twitter.com/streaming/reference/post/statuses/filter
            stream.filter(track=keywords)
            # With stream.filter(follow=IDs) to follow accounts
        except Exception as e:
            print '(%s-%s %s:%s:%s) %s' % (now.month, now.day, now.hour, now.minute, now.second, "Error streaming") 
            print '(%s-%s %s:%s:%s) %s' % (now.month, now.day, now.hour, now.minute, now.second, e.message) 
            sys.stdout.flush()
            time.sleep(1 * 60)
            
        
    
    now = datetime.datetime.now()    
    print '(%s-%s %s:%s:%s) %s' % (now.month, now.day, now.hour, now.minute, now.second, 'Done') 