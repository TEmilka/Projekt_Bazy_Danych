from config import *
from generators import *
from benchmarks.runner import run

from scenarios.insert import *
from scenarios.update import *
from scenarios.delete import *
from scenarios.read import *

def clear_postgres(conn):
    cur = conn.cursor()
    cur.execute("""
        TRUNCATE TABLE sale_items, sales, prescriptions, products, customers, employees CASCADE;
    """)
    conn.commit()

def clear_mysql(conn):
    cur = conn.cursor()

    # wyłącz FK (bo inaczej TRUNCATE się wywali)
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
    db.prescriptions.delete_many({})
    db.sales.delete_many({})
    db.sale_items.delete_many({})
    db.customers.delete_many({})
    db.employees.delete_many({})

def clear_redis(r):
    r.flushdb()

SIZES=[10000,100000,1000000]

def main():
    pg=get_postgres()
    mysql=get_mysql()
    mongo=get_mongo()
    redis_db=get_redis()

    for size in SIZES:
        print("SIZE:",size)
        # prepare datasets
        products = [product(i) for i in range(size)]
        print("Generated products:")
        customers = [customer(i) for i in range(size)]
        print("Generated customers:")
        employees = [employee(i) for i in range(size)]
        print("Generated employees:")
        prescriptions = [prescription(i, size) for i in range(size)]
        print("Generated prescriptions:")
        sales = [sale(i, size) for i in range(size)]
        print("Generated sales:")

        # ---------- INSERTS ----------
        print("Scenarios: INSERT 1")
        run("postgres","INSERT_PRODUCT", lambda x: insert_product_postgres(pg, x), products, "app/results/postgres.csv")
        run("mysql","INSERT_PRODUCT", lambda x: insert_product_mysql(mysql, x), products, "app/results/mysql.csv")
        run("mongo","INSERT_PRODUCT", lambda x: insert_product_mongo(mongo, x), products, "app/results/mongo.csv")
        run("redis","INSERT_PRODUCT", lambda x: insert_product_redis(redis_db, x), products, "app/results/redis.csv")
        
        print("Scenarios: INSERT 2")
        run("postgres","INSERT_PRESCRIPTION", lambda x: insert_prescription_sql(pg, x), prescriptions, "app/results/postgres.csv")
        run("mysql","INSERT_PRESCRIPTION", lambda x: insert_prescription_sql(mysql, x), prescriptions, "app/results/mysql.csv")
        run("mongo","INSERT_PRESCRIPTION", lambda x: insert_prescription_mongo(mongo, x), prescriptions, "app/results/mongo.csv")
        run("redis","INSERT_PRESCRIPTION", lambda x: insert_prescription_redis(redis_db, x), prescriptions, "app/results/redis.csv")

        print("Scenarios: INSERT 3")
        run("postgres","INSERT_SALE", lambda x: insert_sale_sql(pg, x), sales, "app/results/postgres.csv")
        run("mysql","INSERT_SALE", lambda x: insert_sale_sql(mysql, x), sales, "app/results/mysql.csv")
        run("mongo","INSERT_SALE", lambda x: insert_sale_mongo(mongo, x), sales, "app/results/mongo.csv")
        run("redis","INSERT_SALE", lambda x: insert_sale_redis(redis_db, x), sales, "app/results/redis.csv")

        # ---------- UPDATES ----------
        print("Scenarios: UPDATE 1")
        updates = [update_phone(i) for i in range(size)]
        run("postgres","UPDATE_PHONE", lambda x: update_phone_sql(pg, x), updates, "app/results/postgres.csv")
        run("mysql","UPDATE_PHONE", lambda x: update_phone_sql(mysql, x), updates, "app/results/mysql.csv")
        run("mongo","UPDATE_PHONE", lambda x: update_phone_mongo(mongo, x), updates, "app/results/mongo.csv")
        run("redis","UPDATE_PHONE", lambda x: update_phone_redis(redis_db, x), updates, "app/results/redis.csv")

        # promote employees
        print("Scenarios: UPDATE 2")
        promos = [{"id": i, "job_position": "Senior", "salary": 1.2 * employee(i)["salary"] if isinstance(employee(i)["salary"], (int,float)) else 5000} for i in range(size)]
        run("postgres","PROMOTE", lambda x: promote_sql(pg, x), promos, "app/results/postgres.csv")
        run("mysql","PROMOTE", lambda x: promote_sql(mysql, x), promos, "app/results/mysql.csv")
        run("mongo","PROMOTE", lambda x: promote_mongo(mongo, x), promos, "app/results/mongo.csv")
        run("redis","PROMOTE", lambda x: promote_redis(redis_db, x), promos, "app/results/redis.csv")

        # expire prescriptions (single op, scans and updates expired rows)
        # Note: read/write function does not need an argument, runner expects an iterable, so we pass a single placeholder element.
        print("Scenarios: UPDATE 3")
        run("postgres","EXPIRE_PRESCRIPTIONS", lambda x: expire_sql(pg, x), [None], "app/results/postgres.csv")
        run("mysql","EXPIRE_PRESCRIPTIONS", lambda x: expire_sql(mysql, x), [None], "app/results/mysql.csv")
        run("mongo","EXPIRE_PRESCRIPTIONS", lambda x: expire_mongo(mongo, x), [None], "app/results/mongo.csv")



        # dorobic redis !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


        # ---------- READS ----------
        # READ_PRODUCT: test finding one specific product (not all)
        print("Scenarios: READ 1")
        target_idx = size // 2
        product_reads = [{"name": f"Product {target_idx}"}]
        run("postgres","READ_PRODUCT", lambda x: read_product_sql(pg, x), product_reads, "app/results/postgres.csv")
        run("mysql","READ_PRODUCT", lambda x: read_product_sql(mysql, x), product_reads, "app/results/mysql.csv")
        run("mongo","READ_PRODUCT", lambda x: read_product_mongo(mongo, x), product_reads, "app/results/mongo.csv")
        run("redis","READ_PRODUCT", lambda x: read_product_redis(redis_db, x), product_reads, "app/results/redis.csv")

        # READ_SALES: read all info for one specific sale (sale + items + product details)
        print("Scenarios: READ 2")
        target_sale = size // 2
        run("postgres","READ_SALES", lambda x: read_sales_sql(pg, x), [target_sale], "app/results/postgres.csv")
        run("mysql","READ_SALES", lambda x: read_sales_sql(mysql, x), [target_sale], "app/results/mysql.csv")
        run("mongo","READ_SALES", lambda x: read_sales_mongo(mongo, x), [target_sale], "app/results/mongo.csv")
        run("redis","READ_SALES", lambda x: read_sales_redis(redis_db, x), [target_sale], "app/results/redis.csv")

        # READ_PRESCRIPTIONS: for a single customer id, read all their prescriptions
        print("Scenarios: READ 3")
        target_customer = size // 3
        run("postgres","READ_PRESCRIPTIONS", lambda x: read_prescriptions_sql(pg, x), [target_customer], "app/results/postgres.csv")
        run("mysql","READ_PRESCRIPTIONS", lambda x: read_prescriptions_sql(mysql, x), [target_customer], "app/results/mysql.csv")
        run("mongo","READ_PRESCRIPTIONS", lambda x: read_prescriptions_mongo(mongo, x), [target_customer], "app/results/mongo.csv")
        run("redis","READ_PRESCRIPTIONS", lambda x: read_prescriptions_redis(redis_db, x), [target_customer], "app/results/redis.csv")

        # ---------- DELETES ----------
        import random as _rand
        # DELETE: single employee (simulate firing one person)
        emp_to_delete = _rand.randrange(0, size)
        run("postgres","DELETE_EMPLOYEE", lambda x: delete_employee_sql(pg, x), [emp_to_delete], "app/results/postgres.csv")
        run("mysql","DELETE_EMPLOYEE", lambda x: delete_employee_sql(mysql, x), [emp_to_delete], "app/results/mysql.csv")
        run("mongo","DELETE_EMPLOYEE", lambda x: delete_employee_mongo(mongo, x), [emp_to_delete], "app/results/mongo.csv")
        run("redis","DELETE_EMPLOYEE", lambda x: delete_employee_redis(redis_db, x), [emp_to_delete], "app/results/redis.csv")

        # DELETE: remove all closed prescriptions
        run("postgres","DELETE_CLOSED_PRESCRIPTIONS", lambda x: delete_closed_prescriptions_sql(pg), [None], "app/results/postgres.csv")
        run("mysql","DELETE_CLOSED_PRESCRIPTIONS", lambda x: delete_closed_prescriptions_sql(mysql), [None], "app/results/mysql.csv")
        run("mongo","DELETE_CLOSED_PRESCRIPTIONS", lambda x: delete_closed_prescriptions_mongo(mongo), [None], "app/results/mongo.csv")
        run("redis","DELETE_CLOSED_PRESCRIPTIONS", lambda x: delete_closed_prescriptions_redis(redis_db), [None], "app/results/redis.csv")

        # DELETE: one random customer and cascade delete their data
        cust_to_delete = _rand.randrange(0, size)
        run("postgres","DELETE_CUSTOMER", lambda x: delete_customer_sql(pg, x), [cust_to_delete], "app/results/postgres.csv")
        run("mysql","DELETE_CUSTOMER", lambda x: delete_customer_sql(mysql, x), [cust_to_delete], "app/results/mysql.csv")
        run("mongo","DELETE_CUSTOMER", lambda x: delete_customer_mongo(mongo, x), [cust_to_delete], "app/results/mongo.csv")
        run("redis","DELETE_CUSTOMER", lambda x: delete_customer_redis(redis_db, x), [cust_to_delete], "app/results/redis.csv")

        print("Clearing databases...")
        clear_postgres(pg)
        clear_mysql(mysql)
        clear_mongo(mongo)
        clear_redis(redis_db)

if __name__=="__main__":
    main()