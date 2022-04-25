from os import environ

from pymongo.mongo_client import MongoClient

client = MongoClient(environ['MONGODB_DBURL'])
db = client[environ['MONGODB_DBNAME']]
