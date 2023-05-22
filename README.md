# The-Data-Pipeline
A dockerized pipeline using ETL job
- Docker
- MongoDB
- PostgreSQL
- API
- Slackbot

This pipeline includes Docker containers for:
- collecting tweets via the twitter API on a given hashtag
- storing tweets in a MongoDB
- an ETL job that transforms collected tweets and stores metadata into a Postgres database
- a sentiment analysis using Vader module
- a Slackbot which posts tweets to a specific Slack channel



<img width="1280" alt="slackbot" src="https://github.com/v-fruehmann/The-Data-Pipeline/assets/69976662/7bcc25b5-e0a7-44db-bdb8-3503bb0fd9a1">
