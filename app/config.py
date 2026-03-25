import psycopg2
import pymysql
from pymongo import MongoClient
import redis
import os

def get_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        user="admin",
        password="admin",
        dbname="apteka"
    )

def get_mysql():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        user="admin",
        password="admin",
        database="apteka"
    )

def get_mongo():
    return MongoClient(
        f"mongodb://admin:admin@{os.getenv('MONGO_HOST')}:27017/"
    )["pharmacy_db"]

def get_redis():
    return redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=6379,
        password=os.getenv("REDIS_PASSWORD"),
        decode_responses=True
    )