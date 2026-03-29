import json
from datetime import datetime

# PRODUCTS
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

def insert_product_mongo(db,p):
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

def insert_product_redis(r,p):
    r.hset(f"product:{p['id']}", mapping={
        "name": p["name"],
        "is_prescription_required": str(p["is_prescription_required"]),
        "barcode": str(p["barcode"]),
        "price": str(p["price"]),
        "category": p["category"]
    })
    return p["id"]


# PRESCRIPTIONS
def insert_prescription_sql(conn,presc):
    cur=conn.cursor()
    # verify customer exists (avoid FK error and simulate validation)
    cur.execute("SELECT 1 FROM customers WHERE customer_id=%s", (presc["customer_id"],))
    if cur.fetchone() is None:
        return None
    cur.execute(
        "INSERT INTO prescriptions (customer_id,issue_date,expiry_date,status) VALUES (%s,%s,%s,%s) RETURNING prescription_id",
        (presc["customer_id"], presc["issue_date"], presc["expiry_date"], presc["status"])
    )
    try:
        pid = cur.fetchone()[0]
    except Exception:
        pid = cur.lastrowid if hasattr(cur, 'lastrowid') else None
    conn.commit()
    return pid

def insert_prescription_mongo(db,presc):
    # verify customer exists
    if db.customers.find_one({"_id": presc["customer_id"]}) is None:
        return None
    doc = {
        "_id": presc["id"],
        "customer_id": presc["customer_id"],
        "issue_date": presc["issue_date"],
        "expiry_date": presc["expiry_date"],
        "status": presc["status"]
    }
    db.prescriptions.insert_one(doc)
    return doc["_id"]

def insert_prescription_redis(r,presc):
    # verify customer exists in redis
    if not r.exists(f"customer:{presc['customer_id']}"):
        return None
    r.hset(f"prescription:{presc['id']}", mapping={
        "customer_id": str(presc["customer_id"]),
        "issue_date": presc["issue_date"].isoformat() if hasattr(presc["issue_date"], 'isoformat') else str(presc["issue_date"]),
        "expiry_date": presc["expiry_date"].isoformat() if hasattr(presc["expiry_date"], 'isoformat') else str(presc["expiry_date"]),
        "status": presc["status"]
    })
    return presc["id"]


# SALES
def insert_sale_sql(conn,s):
    cur=conn.cursor()
    # verify customer and employee exist
    cur.execute("SELECT 1 FROM customers WHERE customer_id=%s", (s.get("customer_id"),))
    if cur.fetchone() is None:
        return None
    cur.execute("SELECT 1 FROM employees WHERE employee_id=%s", (s.get("employee_id"),))
    if cur.fetchone() is None:
        return None
    # insert sale
    cur.execute(
        "INSERT INTO sales (customer_id,employee_id,sale_date,total_value,status) VALUES (%s,%s,%s,%s,%s) RETURNING sale_id",
        (s.get("customer_id"), s.get("employee_id"), s.get("sale_date"), s.get("total_value"), s.get("status"))
    )
    try:
        sid = cur.fetchone()[0]
    except Exception:
        sid = cur.lastrowid if hasattr(cur, 'lastrowid') else None
    # insert sale items if provided
    items = s.get("products") or []
    for it in items:
        try:
            cur.execute("INSERT INTO sale_items (sale_id,product_id,product_quantity) VALUES (%s,%s,%s)",
                        (sid, it.get("id"), it.get("product_quantity", 1)))
        except Exception:
            # ignore FK failures during bulk generation
            pass
    conn.commit()
    return sid

def insert_sale_mongo(db,s):
    # verify customer and employee exist
    if db.customers.find_one({"_id": s.get("customer_id")}) is None:
        return None
    if db.employees.find_one({"_id": s.get("employee_id")}) is None:
        return None
    sale_doc = {
        "_id": s["id"],
        "customer_id": s.get("customer_id"),
        "employee_id": s.get("employee_id"),
        "sale_date": s.get("sale_date", datetime.utcnow()),
        "total_value": s.get("total_value"),
        "status": s.get("status")
    }
    db.sales.insert_one(sale_doc)
    items = []
    for it in s.get("products", []):
        items.append({
            "sale_id": s["id"],
            "product_id": it.get("id"),
            "product_quantity": it.get("product_quantity", 1)
        })
    if items:
        db.sale_items.insert_many(items)
    return sale_doc["_id"]

def insert_sale_redis(r,s):
    # verify customer and employee exist
    if not r.exists(f"customer:{s.get('customer_id')}"):
        return None
    if not r.exists(f"employee:{s.get('employee_id')}"):
        return None
    sid = s["id"]
    r.hset(f"sale:{sid}", mapping={
        "customer_id": str(s.get("customer_id")),
        "employee_id": str(s.get("employee_id")),
        "sale_date": s.get("sale_date").isoformat() if hasattr(s.get("sale_date"), 'isoformat') else str(s.get("sale_date")),
        "total_value": str(s.get("total_value")),
        "status": s.get("status")
    })
    for it in s.get("products", []):
        r.rpush(f"sale:{sid}:products", json.dumps({
            "product_id": it.get("id"),
            "quantity": it.get("product_quantity", 1),
            "price": it.get("price", None)
        }))
    return sid
