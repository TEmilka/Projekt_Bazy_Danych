import psycopg2
import pymysql
from pymongo import MongoClient
import redis
import os


def get_host(name):
    return os.getenv(name) or "localhost"


# ---------- POSTGRES ----------
def get_postgres():
    return psycopg2.connect(
        host=get_host("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER") or "admin",
        password=os.getenv("POSTGRES_PASSWORD") or "admin",
        dbname=os.getenv("POSTGRES_DB") or "apteka"
    )


# ---------- MYSQL ----------
def get_mysql():
    return pymysql.connect(
        host=get_host("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER") or "admin",
        password=os.getenv("MYSQL_PASSWORD") or "admin",
        database=os.getenv("MYSQL_DATABASE") or "apteka"
    )


# ---------- MONGO ----------
def get_mongo():
    host = get_host("MONGO_HOST")
    user = os.getenv("MONGO_INITDB_ROOT_USERNAME") or "admin"
    password = os.getenv("MONGO_INITDB_ROOT_PASSWORD") or "admin"
    db_name = os.getenv("MONGO_INITDB_DATABASE") or "apteka"

    client = MongoClient(
        f"mongodb://{user}:{password}@{host}:27017/"
    )
    return client[db_name]


# ---------- REDIS ----------
def get_redis():
    return redis.Redis(
        host=get_host("REDIS_HOST"),
        port=6379,
        password=os.getenv("REDIS_PASSWORD") or "admin",
        decode_responses=True
    )