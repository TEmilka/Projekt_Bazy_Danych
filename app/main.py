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
    print(f"\n🔥 SEEDING DATABASE: {size}")

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

    print("✅ SEED DONE\n")

# =========================
# TESTY
# =========================
def run_all_tests(pg, mysql, mongo, redis_db, size):

    print(f"\n🚀 TESTY NA BAZIE: {size}\n")

    # INSERT
    product_data = [product(size + 1)]
    prescription_data = [prescription(size + 1, size)]
    sale_data = [sale(size + 1, size)]

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

    # UPDATE
    update_data = [{"id": size // 2, "phone": "999999999"}]

    run("postgres","UPDATE_PHONE", lambda x: update_phone_sql(pg, x), update_data, "app/results/postgres.csv", size)
    run("mysql","UPDATE_PHONE", lambda x: update_phone_sql(mysql, x), update_data, "app/results/mysql.csv", size)
    run("mongo","UPDATE_PHONE", lambda x: update_phone_mongo(mongo, x), update_data, "app/results/mongo.csv", size)
    run("redis","UPDATE_PHONE", lambda x: update_phone_redis(redis_db, x), update_data, "app/results/redis.csv", size)

    promos = [
        {
            "id": i,
            "job_position": "Senior",
            "salary": 1.2 * employee(i)["salary"]
            if isinstance(employee(i)["salary"], (int, float))
            else 5000
        }
        for i in range(size)
    ]

    run("postgres","PROMOTE", lambda x: promote_sql(pg, x), promos, "app/results/postgres.csv", size)
    run("mysql","PROMOTE", lambda x: promote_sql(mysql, x), promos, "app/results/mysql.csv", size)
    run("mongo","PROMOTE", lambda x: promote_mongo(mongo, x), promos, "app/results/mongo.csv", size)
    run("redis","PROMOTE", lambda x: promote_redis(redis_db, x), promos, "app/results/redis.csv", size)

    run("postgres","EXPIRE_PRESCRIPTIONS", lambda x: expire_sql(pg, x), [None], "app/results/postgres.csv", size)
    run("mysql","EXPIRE_PRESCRIPTIONS", lambda x: expire_sql(mysql, x), [None], "app/results/mysql.csv", size)
    run("mongo","EXPIRE_PRESCRIPTIONS", lambda x: expire_mongo(mongo, x), [None], "app/results/mongo.csv", size)
    run("redis","EXPIRE_PRESCRIPTIONS", lambda x: expire_redis(redis_db, x), [None], "app/results/redis.csv", size)

    # READ
    read_data = [{"name": f"Product {size // 2}"}]

    run("postgres","READ_PRODUCT", lambda x: read_product_sql(pg, x), read_data, "app/results/postgres.csv", size)
    run("mysql","READ_PRODUCT", lambda x: read_product_sql(mysql, x), read_data, "app/results/mysql.csv", size)
    run("mongo","READ_PRODUCT", lambda x: read_product_mongo(mongo, x), read_data, "app/results/mongo.csv", size)
    run("redis","READ_PRODUCT", lambda x: read_product_redis(redis_db, x), read_data, "app/results/redis.csv", size)

    target_sale = size // 2
    run("postgres","READ_SALES", lambda x: read_sales_sql(pg, x), [target_sale], "app/results/postgres.csv", size)
    run("mysql","READ_SALES", lambda x: read_sales_sql(mysql, x), [target_sale], "app/results/mysql.csv", size)
    run("mongo","READ_SALES", lambda x: read_sales_mongo(mongo, x), [target_sale], "app/results/mongo.csv", size)
    run("redis","READ_SALES", lambda x: read_sales_redis(redis_db, x), [target_sale], "app/results/redis.csv", size)

    target_customer = size // 3
    run("postgres","READ_PRESCRIPTIONS", lambda x: read_prescriptions_sql(pg, x), [target_customer], "app/results/postgres.csv", size)
    run("mysql","READ_PRESCRIPTIONS", lambda x: read_prescriptions_sql(mysql, x), [target_customer], "app/results/mysql.csv", size)
    run("mongo","READ_PRESCRIPTIONS", lambda x: read_prescriptions_mongo(mongo, x), [target_customer], "app/results/mongo.csv", size)
    run("redis","READ_PRESCRIPTIONS", lambda x: read_prescriptions_redis(redis_db, x), [target_customer], "app/results/redis.csv", size)

    # DELETE
    delete_id = size // 2

    run("postgres","DELETE_EMPLOYEE", lambda x: delete_employee_sql(pg, x), [delete_id], "app/results/postgres.csv", size)
    run("mysql","DELETE_EMPLOYEE", lambda x: delete_employee_sql(mysql, x), [delete_id], "app/results/mysql.csv", size)
    run("mongo","DELETE_EMPLOYEE", lambda x: delete_employee_mongo(mongo, x), [delete_id], "app/results/mongo.csv", size)
    run("redis","DELETE_EMPLOYEE", lambda x: delete_employee_redis(redis_db, x), [delete_id], "app/results/redis.csv", size)

    run("postgres","DELETE_CLOSED_PRESCRIPTIONS", lambda x: delete_prescription_sql(pg,x), [delete_id], "app/results/postgres.csv", size)
    run("mysql","DELETE_CLOSED_PRESCRIPTIONS", lambda x: delete_prescription_sql(mysql,x), [delete_id], "app/results/mysql.csv", size)
    run("mongo","DELETE_CLOSED_PRESCRIPTIONS", lambda x: delete_prescription_mongo(mongo,x), [delete_id], "app/results/mongo.csv", size)
    run("redis","DELETE_CLOSED_PRESCRIPTIONS", lambda x: delete_prescription_redis(redis_db,x), [delete_id], "app/results/redis.csv", size)

    cust_to_delete = random.randrange(0, size)
    run("postgres","DELETE_CUSTOMER", lambda x: delete_customer_sql(pg, x), [cust_to_delete], "app/results/postgres.csv", size)
    run("mysql","DELETE_CUSTOMER", lambda x: delete_customer_sql(mysql, x), [cust_to_delete], "app/results/mysql.csv", size)
    run("mongo","DELETE_CUSTOMER", lambda x: delete_customer_mongo(mongo, x), [cust_to_delete], "app/results/mongo.csv", size)
    run("redis","DELETE_CUSTOMER", lambda x: delete_customer_redis(redis_db, x), [cust_to_delete], "app/results/redis.csv", size)

# =========================
# MAIN
# =========================
def main():
    sizes = [10,100,1000]

    for SIZE in sizes:
        print(f"\n🚀 START TEST FOR SIZE: {SIZE}")

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

        print(f"✅ DONE SIZE: {SIZE}\n")

if __name__ == "__main__":
    main()