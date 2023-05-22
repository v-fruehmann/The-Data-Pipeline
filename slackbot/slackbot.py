import time
import slack
import logging
from sqlalchemy import create_engine
import psycopg2
import requests
import slack_password 


# authentication
webhook_url =slack_password.webhook_url

# connect to the Postgres database
HOST = 'postgresdb'
PORT = 5432
DATABASE = 'twitter'
USER = 'postgres'
PASSWORD = 'postgres'

conn_string = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
engine = create_engine(conn_string)

def extract():
    tweet_list = []
    query = '''
    SELECT id,text, sentiment_score 
    FROM tweets 
    ORDER BY sentiment_score DESC 
    LIMIT 10;'''
    query_result = engine.execute(query)
    for tweet in query_result:
        tweet_list.append(tweet)
    return tweet_list

logging.basicConfig(level=logging.WARNING)


time.sleep(60)
tweet_list = extract()
for tweet in tweet_list:
    data = {'text': 'id_' + str(tweet['id']) + ': ' + tweet['text'] + "\nThe sentiment score of the tweet is: " + str(tweet['sentiment_score'])}
    requests.post(url = webhook_url, json = data)
    logging.warning('---New tweet has arrived: ')
    

