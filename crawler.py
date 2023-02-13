import tweepy
import csv, os, sys
import threading
import traceback
import re


maxId = -1
tweetCount = 0
tweetIds = set()

def writeTweets(filename, tag, keys, maxTweets):

  consumer_key = keys[0]
  consumer_secret = keys[1]
  access_token = keys[2]
  access_token_secret = keys[3]
  
  tweetsPerQry = maxTweets
  maxTweets = maxTweets
  
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
    fieldNames = ['id', 'tweet', 'time', 'lat', 'long', 'hashtag']  #id,tweet,time,lat,long,hashtag
    writer = csv.DictWriter(csvFile, fieldnames=fieldNames)
    if not fileExists:
        writer.writeheader()
    while tweetCount < maxTweets:
      try:
        if(maxId <= 0):
          newTweets = api.search_tweets(q=tag, count=tweetsPerQry, result_type="recent", tweet_mode="extended")
        else:
          newTweets = api.search_tweets(q=tag, count=tweetsPerQry, max_id=str(maxId - 1), result_type="recent", tweet_mode="extended")
        
        for tweet in newTweets:
            if str(tweet.id) not in tweetIds:
                if tweet.coordinates:
                    lat = tweet.coordinates["coordinates"][1]
                    long = tweet.coordinates["coordinates"][0]
                else:
                    lat = ''
                    long = ''
                tweetBody = str(tweet.full_text.encode(encoding='utf8'))
                tags = re.findall(r'#\w+', tweetBody)
                tagCol = ' '.join(tags)
                hashtag= tagCol
                tweetIds.add(tweet.id)
                val = {'id': tweet.id, 'tweet': tweet.full_text.encode(encoding='utf8'), 'time': tweet.created_at, 'lat': lat, 'long': long, 'hashtag': hashtag}
                writer.writerow(val)
          
        tweetCount += len(newTweets)
        maxId = newTweets[-1].id
      except Exception as e:
        print("Exception:", e)
        print(traceback.format_exc())
        continue
  print("Length of tweets: ", len(tweetIds))

def crawl(key, fileName, hashtag, maxTweets):
  threads = []
  tags = hashtag.split(',')
  for i in range(5):
    filename = fileName + str(i) + '.csv'
    t = threading.Thread(target=writeTweets, args = (filename,tags[i],key, maxTweets))
    threads.append(t)
    t.start()
  
  for t in threads:
    t.join()

keys = ["7j3SDWeoHo1YqTSqQXAQoPifj", "ITFFjSkuGkshQYhOFJpNB4diTM8h3EWVDJQHxbpClADKTNwMwc", "1254426768-MIvdYvzS72RKamMAGG3ovGJDO9zx1xSpag9hiZn", "J04nzhvEt3eNanYHhVluAtQ9SC9H6cnpcSqgoV06zUAAG"]
hashtags = "Jae Crowder,INDvsAUS,Pant,KS Bharat,Jadeja"

if __name__ == '__main__':
    fileName = sys.argv[1]
    maxTweets = int(sys.argv[2])
    crawl(keys, fileName, hashtags, maxTweets)