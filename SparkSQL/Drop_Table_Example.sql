DELETE FROM tabular.dataexpert.cmgoffena13_tweets_old;
VACUUM tabular.dataexpert.cmgoffena13_tweets_old RETAIN 0 HOURS;
DROP TABLE tabular.dataexpert.cmgoffena13_tweets_old;