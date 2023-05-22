from sqlalchemy import create_engine
import pymongo
import time
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import logging


time.sleep(10)

# connect to the MongoDB database
client = pymongo.MongoClient(host="mongodb", port=27017)
db = client.twitter
coll = db.tweets

# connect to the Postgres database
HOST = 'postgresdb'
PORT = 5432
DATABASE = 'twitter'
USER = 'postgres'
PASSWORD = 'postgres'

conn_string = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
engine = create_engine(conn_string)

# preprocessing
mentions_regex = '@[A-Za-z0-9]+'
url_regex = 'https?:\/\/\S+'

# create a table tweets in the Postgres database
create_table = '''
    CREATE TABLE IF NOT EXISTS tweets(
    id BIGINT PRIMARY KEY,
    text VARCHAR(500),
    created_at VARCHAR(500),          
    sentiment_score NUMERIC
);
'''
engine.execute(create_table)

# instantiate Vader
s  = SentimentIntensityAnalyzer()

# extracts tweets from the MongoDB 
def extract():
    extracted_tweets = list(coll.find())[-10:] # last ten tweets are extracted
    return extracted_tweets

# cleans text with RegEx
def regex_clean(text):
    text = re.sub(mentions_regex, '', text)
    text = re.sub(url_regex, '', text)
    text= re.sub('\n',' ', text)

# transform tweets extracted from MongoDB
def transform(extracted_tweets):
    transformed_tweets = []
    for tweet in extracted_tweets:
        text = tweet['text']
        text = regex_clean(text)
        sentiment_score = s.polarity_scores(tweet['text'])['compound']
        print(sentiment_score)
        tweet['sentiment_score'] = sentiment_score
        transformed_tweets.append(tweet)
        print(tweet)
     
    return transformed_tweets

#  loads transformed tweets into the PostgresDB
def load(transformed_tweets):
    for tweet in transformed_tweets:
        insert_query = "INSERT INTO tweets VALUES (%s,%s, %s, %s) ON CONFLICT DO NOTHING"
        engine.execute(insert_query, (tweet['id'], tweet['text'],tweet['created_at'], tweet['sentiment_score']))
        logging.warning('---New tweets inserted into Postgres')

if __name__ == '__main__':
    while True:
        extracted_tweets = extract()
        transformed_tweets = transform(extracted_tweets)
        load(transformed_tweets)
        time.sleep(600)
        
