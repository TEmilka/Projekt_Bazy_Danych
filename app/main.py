from config import *
from generators import *
from benchmarks.runner import run

from scenarios.insert import *
from scenarios.update import *
from scenarios.delete import *
from scenarios.read import *

SIZES=[10000,100000,1000000]

def main():
    pg=get_postgres()
    mysql=get_mysql()
    mongo=get_mongo()
    redis_db=get_redis()

    for size in SIZES:
        print("SIZE:",size)

        products=[product(i) for i in range(size)]
        customers=[customer(i) for i in range(size)]

        # INSERT PRODUCT
        run("pg","INSERT_PRODUCT",lambda x: insert_product_sql(pg,x),products,"results/postgres.csv")
        run("mongo","INSERT_PRODUCT",lambda x: insert_product_mongo(mongo,x),products,"results/mongo.csv")
        run("redis","INSERT_PRODUCT",lambda x: insert_product_redis(redis_db,x),products,"results/redis.csv")

        # UPDATE PHONE
        updates=[update_phone(i) for i in range(size)]
        run("pg","UPDATE_PHONE",lambda x: update_phone_sql(pg,x),updates,"results/postgres.csv")

        # itd dla wszystkich scenariuszy...

if __name__=="__main__":
    main()