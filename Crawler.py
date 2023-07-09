import tweepy
import csv, os
import threading, random
import traceback
import time

maxId = -1
tweetCount = 0
tweetIds = set()

def writeTweets(filename, tag, keys):

  consumer_key = keys[0]
  consumer_secret = keys[1]
  access_token = keys[2]
  access_token_secret = keys[3]
  
  tweetsPerQry = 70000
  maxTweets = 70000
  
  authentication = tweepy.OAuthHandler(consumer_key, consumer_secret)
  authentication.set_access_token(access_token, access_token_secret)
  api = tweepy.API(authentication, wait_on_rate_limit=True)
  global tweetCount, maxId, tweetIds
  fileExists = os.path.exists(filename)

  if fileExists:
    with open(filename, 'r', newline='') as file:
      reader = csv.reader(file)
      next(reader, None)
      for row in reader:
        tweetIds.add(str(row[0]))
  with open(filename, 'a', newline='', encoding='utf8') as csvFile:
    fieldNames = ['id', 'tweet', 'time', 'lat', 'long', 'screen_name']
    writer = csv.DictWriter(csvFile, fieldnames=fieldNames)
    if not fileExists:
        writer.writeheader()
    while tweetCount < maxTweets:
      # print("Count:", tweetCount)
      try:
        if(maxId <= 0):
          # newTweets = api.search(q=hashtag, count=tweetsPerQry, result_type="recent", tweet_mode="extended")
          newTweets = api.search_tweets(q=tag, count=tweetsPerQry, result_type="recent", tweet_mode="extended")
        else:
          newTweets = api.search_tweets(q=tag, count=tweetsPerQry, max_id=str(maxId - 1), result_type="recent", tweet_mode="extended")
        
        # if not newTweets:
        #   print("Tweet Habis")
        #   break
        
        for tweet in newTweets:
          # if str(tweet.id) not in tweetIds and (tweet.coordinates or tweet.user.location):
          if str(tweet.id) not in tweetIds:
            lat = random.uniform(24.396308, 49.384358)
            long = random.uniform(-124.848974, -66.885444)
            tweetIds.add(tweet.id)
            writer.writerow({'id': tweet.id, 'tweet': tweet.full_text.encode(encoding='utf8'), 'time': tweet.created_at, 'lat': lat, 'long': long, 'screen_name': tweet.user.screen_name})
            # print("id:", tweet.id, "tweet:", tweet.full_text.encode(encoding='utf8'), "time:", tweet.created_at, "geo:", tweet.user.location, "enabled:", tweet.user.geo_enabled)
          
        tweetCount += len(newTweets)
        maxId = newTweets[-1].id
      except Exception as e:
        print("Exception:", e)
        print(traceback.format_exc())
        continue
  print("Length of tweets: ", len(tweetIds))

def crawl(key, val, hashtag):
  threads = []
  # hashtag = "Rashford,Beasley,Westbrook,INDvAUS,Martinez"
  tags = hashtag.split(',')
  for i in range(5):
    filename = "Dir" + str(val) + "/twitterSportsData" + str(i) + '.csv'
    t = threading.Thread(target=writeTweets, args = (filename,tags[i],key))
    threads.append(t)
    t.start()
  
  for t in threads:
    t.join()

keys = ["7j3SDWeoHo1YqTSqQXAQoPifj", "ITFFjSkuGkshQYhOFJpNB4diTM8h3EWVDJQHxbpClADKTNwMwc", "1254426768-MIvdYvzS72RKamMAGG3ovGJDO9zx1xSpag9hiZn", "J04nzhvEt3eNanYHhVluAtQ9SC9H6cnpcSqgoV06zUAAG"]
# hashtags = "Man United,Laker,BGT2023,OMPSG,Utah"
hashtags = "Jae Crowder,INDvsAUS,Pant,KS Bharat,Jadeja"
crawl(keys, '0', hashtags)