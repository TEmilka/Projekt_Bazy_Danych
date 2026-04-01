from config import *
from generators import *
from benchmarks.runner import run

from scenarios.insert import *
from scenarios.update import *
from scenarios.delete import *
from scenarios.read import *

import random

# =========================
# CLEAR DATABASES
# =========================
def clear_postgres(conn):
    cur = conn.cursor()
    cur.execute("""
        TRUNCATE TABLE 
            sale_items, 
            sales, 
            prescriptions, 
            products, 
            customers, 
            employees 
        RESTART IDENTITY CASCADE;
    """)
    conn.commit()

def clear_mysql(conn):
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    cur.execute("TRUNCATE TABLE sale_items;")
    cur.execute("TRUNCATE TABLE sales;")
    cur.execute("TRUNCATE TABLE prescriptions;")
    cur.execute("TRUNCATE TABLE products;")
    cur.execute("TRUNCATE TABLE customers;")
    cur.execute("TRUNCATE TABLE employees;")
    cur.execute("SET FOREIGN_KEY_CHECKS=1;")
    conn.commit()

def clear_mongo(db):
    db.products.delete_many({})
    db.customers.delete_many({})
    db.employees.delete_many({})
    db.prescriptions.delete_many({})
    db.sales.delete_many({})

def clear_redis(r):
    r.flushdb()

# =========================
# SEED
# =========================
def seed_database(pg, mysql, mongo, redis_db, size):
    print(f"\nSEEDING DATABASE: {size}")

    for i in range(size):
        p = product(i)
        c = customer(i)
        e = employee(i)

        insert_product_postgres(pg, p)
        insert_product_mysql(mysql, p)
        insert_product_mongo(mongo, p)
        insert_product_redis(redis_db, p)

        insert_customer_postgres(pg, c)
        insert_customer_mysql(mysql, c)
        insert_customer_mongo(mongo, c)
        insert_customer_redis(redis_db, c)

        insert_employee_postgres(pg, e)
        insert_employee_mysql(mysql, e)
        insert_employee_mongo(mongo, e)
        insert_employee_redis(redis_db, e)

    for i in range(size):
        presc = prescription(i, size)
        s = sale(i, size)

        insert_prescription_postgres(pg, presc)
        insert_prescription_mysql(mysql, presc)
        insert_prescription_mongo(mongo, presc)
        insert_prescription_redis(redis_db, presc)

        insert_sale_postgres(pg, s)
        insert_sale_mysql(mysql, s)
        insert_sale_mongo(mongo, s)
        insert_sale_redis(redis_db, s)

    print("SEED DONE\n")

# =========================
# SCENARIOS
# =========================
def run_all_tests(pg, mysql, mongo, redis_db, size):

    print(f"\nTESTY NA BAZIE: {size}\n")

    # =========================
    # INSERT
    # =========================
    product_data = [lambda: product(random.randint(100000, 999999))]
    prescription_data = [lambda: prescription(random.randint(100000, 999999), size)]
    sale_data = [lambda: sale(random.randint(100000, 999999), size)]

    run("postgres","INSERT_PRODUCT", lambda x: insert_product_postgres(pg, x), product_data, "app/results/postgres.csv", size)
    run("mysql","INSERT_PRODUCT", lambda x: insert_product_mysql(mysql, x), product_data, "app/results/mysql.csv", size)
    run("mongo","INSERT_PRODUCT", lambda x: insert_product_mongo(mongo, x), product_data, "app/results/mongo.csv", size)
    run("redis","INSERT_PRODUCT", lambda x: insert_product_redis(redis_db, x), product_data, "app/results/redis.csv", size)

    run("postgres","INSERT_PRESCRIPTION", lambda x: insert_prescription_postgres(pg, x), prescription_data, "app/results/postgres.csv", size)
    run("mysql","INSERT_PRESCRIPTION", lambda x: insert_prescription_mysql(mysql, x), prescription_data, "app/results/mysql.csv", size)
    run("mongo","INSERT_PRESCRIPTION", lambda x: insert_prescription_mongo(mongo, x), prescription_data, "app/results/mongo.csv", size)
    run("redis","INSERT_PRESCRIPTION", lambda x: insert_prescription_redis(redis_db, x), prescription_data, "app/results/redis.csv", size)

    run("postgres","INSERT_SALE", lambda x: insert_sale_postgres(pg, x), sale_data, "app/results/postgres.csv", size)
    run("mysql","INSERT_SALE", lambda x: insert_sale_mysql(mysql, x), sale_data, "app/results/mysql.csv", size)
    run("mongo","INSERT_SALE", lambda x: insert_sale_mongo(mongo, x), sale_data, "app/results/mongo.csv", size)
    run("redis","INSERT_SALE", lambda x: insert_sale_redis(redis_db, x), sale_data, "app/results/redis.csv", size)

    # =========================
    # UPDATE
    # =========================
    run("postgres","UPDATE_PRICE_CATEGORY", lambda x: update_price_by_category_sql(pg, x), [None], "app/results/postgres.csv", size)
    run("mysql","UPDATE_PRICE_CATEGORY", lambda x: update_price_by_category_sql(mysql, x), [None], "app/results/mysql.csv", size)
    run("mongo","UPDATE_PRICE_CATEGORY", lambda x: update_price_by_category_mongo(mongo, x), [None], "app/results/mongo.csv", size)
    run("redis","UPDATE_PRICE_CATEGORY", lambda x: update_price_by_category_redis(redis_db, x), [None], "app/results/redis.csv", size)

    run("postgres","PROMOTE", lambda x: promote_sql(pg, x), [{"id": size//2, "job_position": "Senior", "salary": 9000}], "app/results/postgres.csv", size)
    run("mysql","PROMOTE", lambda x: promote_sql(mysql, x), [{"id": size//2, "job_position": "Senior", "salary": 9000}], "app/results/mysql.csv", size)
    run("mongo","PROMOTE", lambda x: promote_mongo(mongo, x), [{"id": size//2, "job_position": "Senior", "salary": 9000}], "app/results/mongo.csv", size)
    run("redis","PROMOTE", lambda x: promote_redis(redis_db, x), [{"id": size//2, "job_position": "Senior", "salary": 9000}], "app/results/redis.csv", size)

    run("postgres","EXPIRE_PRESCRIPTIONS", lambda x: expire_sql(pg, x), [None], "app/results/postgres.csv", size)
    run("mysql","EXPIRE_PRESCRIPTIONS", lambda x: expire_sql(mysql, x), [None], "app/results/mysql.csv", size)
    run("mongo","EXPIRE_PRESCRIPTIONS", lambda x: expire_mongo(mongo, x), [None], "app/results/mongo.csv", size)
    run("redis","EXPIRE_PRESCRIPTIONS", lambda x: expire_redis(redis_db, x), [None], "app/results/redis.csv", size)

    # =========================
    # READ
    # =========================
    run("postgres","READ_FILTER", lambda x: read_product_filtered_sql(pg, x), [None], "app/results/postgres.csv", size)
    run("mysql","READ_FILTER", lambda x: read_product_filtered_sql(mysql, x), [None], "app/results/mysql.csv", size)
    run("mongo","READ_FILTER", lambda x: read_product_filtered_mongo(mongo, x), [None], "app/results/mongo.csv", size)
    run("redis","READ_FILTER", lambda x: read_product_filtered_redis(redis_db, x), [None], "app/results/redis.csv", size)

    run("postgres","READ_JOIN", lambda x: read_sales_join_sql(pg, size//2), [None], "app/results/postgres.csv", size)
    run("mysql","READ_JOIN", lambda x: read_sales_join_sql(mysql, size//2), [None], "app/results/mysql.csv", size)
    run("mongo","READ_JOIN", lambda x: read_sales_join_mongo(mongo, size//2), [None], "app/results/mongo.csv", size)
    run("redis","READ_JOIN", lambda x: read_sales_join_redis(redis_db, size//2), [None], "app/results/redis.csv", size)

    run("postgres","READ_AGGREGATION", lambda x: read_aggregation_sql(pg, x), [None], "app/results/postgres.csv", size)
    run("mysql","READ_AGGREGATION", lambda x: read_aggregation_sql(mysql, x), [None], "app/results/mysql.csv", size)
    run("mongo","READ_AGGREGATION", lambda x: read_aggregation_mongo(mongo, x), [None], "app/results/mongo.csv", size)
    run("redis","READ_AGGREGATION", lambda x: read_aggregation_redis(redis_db, x), [None], "app/results/redis.csv", size)

    # =========================
    # DELETE
    # =========================
    run("postgres","DELETE_STATUS", lambda x: delete_sales_by_status_sql(pg, x), [None], "app/results/postgres.csv", size)
    run("mysql","DELETE_STATUS", lambda x: delete_sales_by_status_sql(mysql, x), [None], "app/results/mysql.csv", size)
    run("mongo","DELETE_STATUS", lambda x: delete_sales_by_status_mongo(mongo, x), [None], "app/results/mongo.csv", size)
    run("redis","DELETE_STATUS", lambda x: delete_sales_by_status_redis(redis_db, x), [None], "app/results/redis.csv", size)

    run("postgres","DELETE_HIGH_SALARY", lambda x: delete_high_salary_employees_sql(pg, x), [None], "app/results/postgres.csv", size)
    run("mysql","DELETE_HIGH_SALARY", lambda x: delete_high_salary_employees_sql(mysql, x), [None], "app/results/mysql.csv", size)
    run("mongo","DELETE_HIGH_SALARY", lambda x: delete_high_salary_employees_mongo(mongo, x), [None], "app/results/mongo.csv", size)
    run("redis","DELETE_HIGH_SALARY", lambda x: delete_high_salary_employees_redis(redis_db, x), [None], "app/results/redis.csv", size)

    run("postgres","DELETE_OLD_SALES", lambda x: delete_old_sales_postgres(pg, x), [None], "app/results/postgres.csv", size)
    run("mysql","DELETE_OLD_SALES", lambda x: delete_old_sales_mysql(mysql, x), [None], "app/results/mysql.csv", size)
    run("mongo","DELETE_OLD_SALES", lambda x: delete_old_sales_mongo(mongo, x), [None], "app/results/mongo.csv", size)
    run("redis","DELETE_OLD_SALES", lambda x: delete_old_sales_redis(redis_db, x), [None], "app/results/redis.csv", size)

# =========================
# MAIN
# =========================
def main():
    sizes = [10,100,1000]

    for SIZE in sizes:
        print(f"\nSTART TEST FOR SIZE: {SIZE}")

        pg = get_postgres()
        mysql = get_mysql()
        mongo = get_mongo()
        redis_db = get_redis()

        clear_postgres(pg)
        clear_mysql(mysql)
        clear_mongo(mongo)
        clear_redis(redis_db)

        seed_database(pg, mysql, mongo, redis_db, SIZE)
        run_all_tests(pg, mysql, mongo, redis_db, SIZE)

        print(f"DONE SIZE: {SIZE}\n")

if __name__ == "__main__":
    main()