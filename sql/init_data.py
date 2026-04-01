import os
from app.generators import product, customer, employee, prescription, sale

import psycopg2
import pymysql

BATCH = 1000

def populate_postgres(host=None, size=10000):
    host = host or os.getenv('POSTGRES_HOST','localhost')
    conn = psycopg2.connect(host=host, user='admin', password='admin', dbname='apteka')
    cur = conn.cursor()

    batch = []
    for i in range(size):
        c = customer(i)
        batch.append((c['name'], c['surname'], c['pesel'], c['phone']))
        if len(batch) >= BATCH:
            cur.executemany("INSERT INTO customers (name,surname,pesel,phone) VALUES (%s,%s,%s,%s)", batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany("INSERT INTO customers (name,surname,pesel,phone) VALUES (%s,%s,%s,%s)", batch)
        conn.commit()

    batch = []
    for i in range(size):
        p = product(i)
        batch.append((p['name'], p['is_prescription_required'], p['barcode'], p['price'], p['category']))
        if len(batch) >= BATCH:
            cur.executemany("INSERT INTO products (name,is_prescription_required,barcode,price,category) VALUES (%s,%s,%s,%s,%s)", batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany("INSERT INTO products (name,is_prescription_required,barcode,price,category) VALUES (%s,%s,%s,%s,%s)", batch)
        conn.commit()

    batch = []
    for i in range(size):
        e = employee(i)
        batch.append((e['name'], e['surname'], e['phone'], e['job_position'], e['salary']))
        if len(batch) >= BATCH:
            cur.executemany("INSERT INTO employees (name,surname,phone,job_position,salary) VALUES (%s,%s,%s,%s,%s)", batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany("INSERT INTO employees (name,surname,phone,job_position,salary) VALUES (%s,%s,%s,%s,%s)", batch)
        conn.commit()

    batch = []
    for i in range(size):
        pr = prescription(i)
        batch.append((pr['customer_id'], pr['issue_date'], pr['expiry_date'], pr['status']))
        if len(batch) >= BATCH:
            cur.executemany("INSERT INTO prescriptions (customer_id,issue_date,expiry_date,status) VALUES (%s,%s,%s,%s)", batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany("INSERT INTO prescriptions (customer_id,issue_date,expiry_date,status) VALUES (%s,%s,%s,%s)", batch)
        conn.commit()

    for i in range(size):
        s = sale(i)
        cur.execute("INSERT INTO sales (customer_id,employee_id,sale_date,total_value,status) VALUES (%s,%s,%s,%s,%s) RETURNING sale_id",
                    (s['customer_id'], s['employee_id'], s['sale_date'], s['total_value'], s['status']))
        try:
            sid = cur.fetchone()[0]
        except Exception:
            sid = None
        for it in s['products']:
            try:
                cur.execute("INSERT INTO sale_items (sale_id,product_id,product_quantity) VALUES (%s,%s,%s)", (sid, it['id'], it.get('product_quantity',1)))
            except Exception:
                pass
        if i % BATCH == 0:
            conn.commit()
    conn.commit()
    print(f"Populated postgres with {size} rows per logical collection")


def populate_mysql(host=None, size=10000):
    host = host or os.getenv('MYSQL_HOST','localhost')
    conn = pymysql.connect(host=host, user='admin', password='admin', database='apteka')
    cur = conn.cursor()

    batch = []
    for i in range(size):
        c = customer(i)
        batch.append((c['name'], c['surname'], c['pesel'], c['phone']))
        if len(batch) >= BATCH:
            cur.executemany("INSERT INTO customers (name,surname,pesel,phone) VALUES (%s,%s,%s,%s)", batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany("INSERT INTO customers (name,surname,pesel,phone) VALUES (%s,%s,%s,%s)", batch)
        conn.commit()

    batch = []
    for i in range(size):
        p = product(i)
        batch.append((p['name'], p['is_prescription_required'], p['barcode'], p['price'], p['category']))
        if len(batch) >= BATCH:
            cur.executemany("INSERT INTO products (name,is_prescription_required,barcode,price,category) VALUES (%s,%s,%s,%s,%s)", batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany("INSERT INTO products (name,is_prescription_required,barcode,price,category) VALUES (%s,%s,%s,%s,%s)", batch)
        conn.commit()

    batch = []
    for i in range(size):
        e = employee(i)
        batch.append((e['name'], e['surname'], e['phone'], e['job_position'], e['salary']))
        if len(batch) >= BATCH:
            cur.executemany("INSERT INTO employees (name,surname,phone,job_position,salary) VALUES (%s,%s,%s,%s,%s)", batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany("INSERT INTO employees (name,surname,phone,job_position,salary) VALUES (%s,%s,%s,%s,%s)", batch)
        conn.commit()

    batch = []
    for i in range(size):
        pr = prescription(i)
        batch.append((pr['customer_id'], pr['issue_date'], pr['expiry_date'], pr['status']))
        if len(batch) >= BATCH:
            cur.executemany("INSERT INTO prescriptions (customer_id,issue_date,expiry_date,status) VALUES (%s,%s,%s,%s)", batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany("INSERT INTO prescriptions (customer_id,issue_date,expiry_date,status) VALUES (%s,%s,%s,%s)", batch)
        conn.commit()

    for i in range(size):
        s = sale(i)
        cur.execute("INSERT INTO sales (customer_id,employee_id,sale_date,total_value,status) VALUES (%s,%s,%s,%s,%s)",
                    (s['customer_id'], s['employee_id'], s['sale_date'], s['total_value'], s['status']))
        sid = cur.lastrowid
        for it in s['products']:
            try:
                cur.execute("INSERT INTO sale_items (sale_id,product_id,product_quantity) VALUES (%s,%s,%s)", (sid, it['id'], it.get('product_quantity',1)))
            except Exception:
                pass
        if i % BATCH == 0:
            conn.commit()
    conn.commit()
    print(f"Populated mysql with {size} rows per logical collection")

if __name__ == '__main__':
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    populate_postgres(size=n)
