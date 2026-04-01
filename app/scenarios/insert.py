import json
from datetime import datetime

# =========================
# PRODUCTS
# =========================
def insert_product_postgres(conn, p):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO products (name,is_prescription_required,barcode,price,category) VALUES (%s,%s,%s,%s,%s) RETURNING product_id",
        (p["name"], p["is_prescription_required"], p["barcode"], p["price"], p["category"])
    )
    pid = cur.fetchone()[0]
    conn.commit()
    return pid


def insert_product_mysql(conn, p):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO products (name,is_prescription_required,barcode,price,category) VALUES (%s,%s,%s,%s,%s)",
        (p["name"], p["is_prescription_required"], p["barcode"], p["price"], p["category"])
    )
    conn.commit()
    return cur.lastrowid


def insert_product_mongo(db, p):
    doc = {
        "_id": p["id"],
        "name": p["name"],
        "is_prescription_required": p["is_prescription_required"],
        "barcode": p["barcode"],
        "price": p["price"],
        "category": p["category"]
    }
    db.products.insert_one(doc)
    return doc["_id"]


def insert_product_redis(r, p):
    r.hset(f"product:{p['id']}", mapping={
        "name": p["name"],
        "is_prescription_required": str(p["is_prescription_required"]),
        "barcode": str(p["barcode"]),
        "price": str(p["price"]),
        "category": p["category"]
    })
    return p["id"]


# =========================
# CUSTOMERS
# =========================
def insert_customer_postgres(conn, c):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO customers (name, surname, pesel, phone) VALUES (%s,%s,%s,%s)",
        (c["name"], c["surname"], c["pesel"], c["phone"])
    )
    conn.commit()


def insert_customer_mysql(conn, c):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO customers (name, surname, pesel, phone) VALUES (%s,%s,%s,%s)",
        (c["name"], c["surname"], c["pesel"], c["phone"])
    )
    conn.commit()


def insert_customer_mongo(db, c):
    db.customers.insert_one({
        "_id": c["id"],
        "name": c["name"],
        "surname": c["surname"],
        "pesel": c["pesel"],
        "phone": c["phone"]
    })


def insert_customer_redis(r, c):
    r.hset(f"customer:{c['id']}", mapping=c)


# =========================
# EMPLOYEES
# =========================
def insert_employee_postgres(conn, e):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO employees (name, surname, phone, job_position, salary) VALUES (%s,%s,%s,%s,%s)",
        (e["name"], e["surname"], e["phone"], e["job_position"], e["salary"])
    )
    conn.commit()


def insert_employee_mysql(conn, e):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO employees (name, surname, phone, job_position, salary) VALUES (%s,%s,%s,%s,%s)",
        (e["name"], e["surname"], e["phone"], e["job_position"], e["salary"])
    )
    conn.commit()


def insert_employee_mongo(db, e):
    db.employees.insert_one({
        "_id": e["id"],
        "name": e["name"],
        "surname": e["surname"],
        "phone": e["phone"],
        "job_position": e["job_position"],
        "salary": e["salary"]
    })


def insert_employee_redis(r, e):
    r.hset(f"employee:{e['id']}", mapping={
        **e,
        "salary": str(e["salary"])
    })


# =========================
# PRESCRIPTIONS
# =========================
def insert_prescription_postgres(conn, presc):
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM customers WHERE customer_id=%s", (presc["customer_id"],))
    if cur.fetchone() is None:
        return None

    cur.execute(
        "INSERT INTO prescriptions (customer_id,issue_date,expiry_date,status) VALUES (%s,%s,%s,%s) RETURNING prescription_id",
        (presc["customer_id"], presc["issue_date"], presc["expiry_date"], presc["status"])
    )

    pid = cur.fetchone()[0]
    conn.commit()
    return pid


def insert_prescription_mysql(conn, presc):
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM customers WHERE customer_id=%s", (presc["customer_id"],))
    if cur.fetchone() is None:
        return None

    cur.execute(
        "INSERT INTO prescriptions (customer_id,issue_date,expiry_date,status) VALUES (%s,%s,%s,%s)",
        (presc["customer_id"], presc["issue_date"], presc["expiry_date"], presc["status"])
    )

    conn.commit()
    return cur.lastrowid


def insert_prescription_mongo(db, presc):
    if db.customers.find_one({"_id": presc["customer_id"]}) is None:
        return None

    db.prescriptions.insert_one({
        "_id": presc["id"],
        **presc
    })


def insert_prescription_redis(r, presc):
    if not r.exists(f"customer:{presc['customer_id']}"):
        return None

    r.hset(f"prescription:{presc['id']}", mapping={
        "customer_id": str(presc["customer_id"]),
        "issue_date": presc["issue_date"].isoformat(),
        "expiry_date": presc["expiry_date"].isoformat(),
        "status": presc["status"]
    })


# =========================
# SALES
# =========================
def insert_sale_postgres(conn, s):
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM customers WHERE customer_id=%s", (s["customer_id"],))
    if cur.fetchone() is None:
        return None

    cur.execute("SELECT 1 FROM employees WHERE employee_id=%s", (s["employee_id"],))
    if cur.fetchone() is None:
        return None

    cur.execute(
        "INSERT INTO sales (customer_id,employee_id,sale_date,total_value,status) VALUES (%s,%s,%s,%s,%s) RETURNING sale_id",
        (s["customer_id"], s["employee_id"], s["sale_date"], s["total_value"], s["status"])
    )

    sid = cur.fetchone()[0]

    for it in s.get("products", []):
        cur.execute(
            "INSERT INTO sale_items (sale_id,product_id,product_quantity) VALUES (%s,%s,%s)",
            (sid, it.get("id"), it.get("product_quantity", 1))
        )

    conn.commit()
    return sid


def insert_sale_mysql(conn, s):
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO sales (customer_id,employee_id,sale_date,total_value,status) VALUES (%s,%s,%s,%s,%s)",
        (s["customer_id"], s["employee_id"], s["sale_date"], s["total_value"], s["status"])
    )

    sid = cur.lastrowid

    for it in s.get("products", []):
        cur.execute(
            "INSERT INTO sale_items (sale_id,product_id,product_quantity) VALUES (%s,%s,%s)",
            (sid, it.get("id"), it.get("product_quantity", 1))
        )

    conn.commit()
    return sid


def insert_sale_mongo(db, s):
    db.sales.insert_one({
        "_id": s["id"],
        **s
    })


def insert_sale_redis(r, s):
    sid = s["id"]

    r.hset(f"sale:{sid}", mapping={
        "customer_id": str(s["customer_id"]),
        "employee_id": str(s["employee_id"]),
        "sale_date": s["sale_date"].isoformat(),
        "total_value": str(s["total_value"]),
        "status": s["status"]
    })

    for it in s.get("products", []):
        r.rpush(f"sale:{sid}:products", json.dumps(it))