# 1. produkt
def insert_product_sql(conn, p):
    cur = conn.cursor()
    cur.execute("INSERT INTO products (name,is_prescription_required,barcode,price,category) VALUES (%s,%s,%s,%s,%s)",
                (p["name"],p["is_prescription_required"],p["barcode"],p["price"],p["category"]))
    conn.commit()

def insert_product_mongo(db, p):
    db.products.insert_one(p)

def insert_product_redis(r, p):
    r.hset(f"product:{p['id']}", mapping=p)


# 2. recepta (walidacja klienta)
def insert_prescription_sql(conn, p):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM customers WHERE customer_id=%s",(p["customer_id"],))
    if cur.fetchone():
        cur.execute("INSERT INTO prescriptions (customer_id,issue_date,expiry_date,status) VALUES (%s,%s,%s,%s)",
                    (p["customer_id"],p["issue_date"],p["expiry_date"],p["status"]))
        conn.commit()

def insert_prescription_mongo(db, p):
    if db.customers.find_one({"_id": p["customer_id"]}):
        db.prescriptions.insert_one(p)

def insert_prescription_redis(r, p):
    if r.exists(f"customer:{p['customer_id']}"):
        r.hset(f"prescription:{p['id']}", mapping=p)


# 3. sprzedaż
def insert_sale_sql(conn, s):
    cur = conn.cursor()
    cur.execute("INSERT INTO sales (customer_id,employee_id,total_value,status) VALUES (%s,%s,%s,%s)",
                (s["customer_id"],s["employee_id"],s["total_value"],s["status"]))
    conn.commit()

def insert_sale_mongo(db, s):
    db.sales.insert_one(s)

def insert_sale_redis(r, s):
    r.hset(f"sale:{s['id']}", mapping=s)